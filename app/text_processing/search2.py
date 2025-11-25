"""
app/text_processing/parallel_break_loader.py

FULL PIPELINE:

1. Load raw text 
2. Break into chunks 
3. Assign UIDs
4. Compute text_hash for deduplication
5. Score using rules
6. Save to DB (if enabled) — only unique chunks
7. Return processed results
"""

import os
import uuid
import logging
from typing import List, Dict, Any, Optional, Union

# Imports from your project
from app.text_processing.text_breaker import break_text_into_groups
from app.checker.rules import load_rules
from app.checker.checker2 import Checker
from app.storage.storage2 import Storage

from app.utils import (
    get_env, 
    get_logger,
    compute_text_hash        # <-- IMPORTANT NEW IMPORT
)

logger = get_logger(__name__, level=logging.INFO)


# ------------------------------------------------------------
# Internal helper: Convert text(s) → chunks
# ------------------------------------------------------------
def _make_items_from_texts(
    texts: List[str],
    group_size: int = 500
) -> List[Dict[str, Any]]:
    """
    Convert raw texts into chunked items with uid + text + text_hash.
    """
    items = []

    for t_index, text in enumerate(texts):
        chunks = break_text_into_groups(text, group_size=group_size)

        for c_index, chunk in enumerate(chunks):
            uid = f"{t_index}-{c_index}-{uuid.uuid4().hex[:8]}"
            text_hash = compute_text_hash(chunk)

            items.append({
                "uid": uid,
                "text": chunk,
                "text_hash": text_hash
            })

    logger.info("Prepared %d chunk items from %d texts", len(items), len(texts))
    return items


# ------------------------------------------------------------
# CORE PIPELINE: Deduplicate + Score in parallel
# ------------------------------------------------------------
def parallel_process_text(
    text_or_texts: Union[str, List[str]],
    group_size: int = 500,
    rules_path: Optional[str] = None,
    storage: Optional[Storage] = None,
    max_workers: int = 6,
    save: bool = False
) -> List[Dict[str, Any]]:
    """
    Main API for the full pipeline:
    Break → Deduplicate → Score → Save → Return
    """

    # Normalize input
    texts = [text_or_texts] if isinstance(text_or_texts, str) else list(text_or_texts)
    if not texts:
        logger.warning("parallel_process_text() received empty input")
        return []

    # Load rules
    if rules_path:
        if not os.path.exists(rules_path):
            raise FileNotFoundError(f"Rules file not found: {rules_path}")
        rules = load_rules(rules_path)
        logger.info("Loaded %d rules from %s", len(rules), rules_path)
    else:
        logger.warning("No rules.json provided — returning unscored chunks")
        return _make_items_from_texts(texts, group_size)

    # Setup storage if save=True
    if save and storage is None:
        db_path = get_env("DB_PATH", "checks.db")
        storage = Storage(db_path=db_path)
        logger.info("Storage initialized at %s", db_path)

    # Chunk texts
    items = _make_items_from_texts(texts, group_size=group_size)

    # DEDUPLICATION BEFORE SCORING
    unique_items = []
    skipped_count = 0

    for item in items:
        text_hash = item["text_hash"]

        if save and storage.exists_hash(text_hash):
            skipped_count += 1
            continue   # Skip duplicate chunk

        unique_items.append(item)

    logger.info(
        "Deduplication complete: %d unique chunks, %d skipped duplicates",
        len(unique_items), skipped_count
    )

    # Run scoring
    checker = Checker(rules=rules, storage=storage, max_workers=max_workers)
    results = checker.run_checks(unique_items, save=save)

    logger.info("Parallel scoring completed: %d items saved/returned.", len(results))
    return results


# ------------------------------------------------------------
# Pipeline for loading entire folder
# ------------------------------------------------------------
def pipeline_from_folder(
    folder_path: str,
    group_size: int = 500,
    rules_path: Optional[str] = None,
    storage: Optional[Storage] = None,
    max_workers: int = 6,
    save: bool = False,
    file_ext: str = ".txt"
) -> List[Dict[str, Any]]:
    """
    Load all .txt files → chunk → deduplicate → score.
    """

    if not os.path.isdir(folder_path):
        raise NotADirectoryError(f"Not a directory: {folder_path}")

    texts = []

    for fname in sorted(os.listdir(folder_path)):
        if not fname.lower().endswith(file_ext):
            continue

        fpath = os.path.join(folder_path, fname)
        try:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as fh:
                texts.append(fh.read())
        except Exception as e:
            logger.exception("Failed reading %s: %s", fpath, e)

    logger.info("Loaded %d text files from %s", len(texts), folder_path)

    return parallel_process_text(
        texts,
        group_size=group_size,
        rules_path=rules_path,
        storage=storage,
        max_workers=max_workers,
        save=save
    )


# ------------------------------------------------------------
# CLI Demo
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parallel Break + Load + Score Pipeline")
    parser.add_argument("--text-file", help="Input text file")
    parser.add_argument("--folder", help="Folder with .txt files")
    parser.add_argument("--rules", default="data/rules.json", help="Path to rules.json")
    parser.add_argument("--save", action="store_true", help="Save results to DB")
    parser.add_argument("--group-size", type=int, default=500)
    parser.add_argument("--max-workers", type=int, default=6)
    args = parser.parse_args()

    if args.folder:
        results = pipeline_from_folder(
            args.folder,
            group_size=args.group_size,
            rules_path=args.rules,
            save=args.save,
            max_workers=args.max_workers
        )
    elif args.text_file:
        with open(args.text_file, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
        results = parallel_process_text(
            text,
            group_size=args.group_size,
            rules_path=args.rules,
            save=args.save,
            max_workers=args.max_workers
        )
    else:
        print("Provide --text-file or --folder")
        results = []

    print(f"Processed {len(results)} unique chunks.")
    if results:
        print("Sample output:", results[0])
