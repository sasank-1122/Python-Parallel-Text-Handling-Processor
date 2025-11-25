"""
app/text_processing/parallel_break_loader.py

This module creates a FULL PIPELINE that performs:

    1. Load text (string or list of strings)
    2. Break text into groups/chunks
    3. Assign UIDs to each chunk
    4. Run rule-based scoring in parallel (using Checker)
    5. Optionally save results to SQLite
    6. Return processed results

This is the missing “Parallel Text Break + Load + Score” pipeline required by your project.
Students can clearly understand the flow since each step is modular.
"""

import os
import uuid
import logging
from typing import List, Dict, Any, Optional, Union

# ✅ FIXED IMPORT PATHS
from app.text_processing.text_breaker import break_text_into_groups
from app.checker.rules import load_rules
from app.checker.checker import Checker
from app.storage.storage import Storage

from app.utils import get_env, get_logger

logger = get_logger(__name__, level=logging.INFO)


# ------------------------------------------------------------
# Internal helper: Convert text(s) → chunk groups → items
# ------------------------------------------------------------
def _make_items_from_texts(
    texts: List[str],
    group_size: int = 500
) -> List[Dict[str, Any]]:
    """
    Convert raw texts into chunked items with unique UIDs.

    Output format:
        [
            { "uid": "...", "text": "..." },
            ...
        ]
    """
    items = []

    for t_index, text in enumerate(texts):
        chunks = break_text_into_groups(text, group_size=group_size)

        for c_index, chunk in enumerate(chunks):
            uid = f"{t_index}-{c_index}-{uuid.uuid4().hex[:8]}"
            items.append({"uid": uid, "text": chunk})

    logger.info("Prepared %d chunk items from %d text inputs", len(items), len(texts))
    return items


# ------------------------------------------------------------
# CORE PIPELINE: Break + Score in parallel
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
    Main API for the full pipeline.

    Args:
        text_or_texts (str or List[str]): The text(s) to process.
        group_size (int): Words per chunk.
        rules_path (str): Path to rules.json
        storage (Storage): DB instance.
        max_workers (int): Parallel workers.
        save (bool): Whether to save results to database.

    Returns:
        List of scored items.
    """

    # Normalize input → list
    if isinstance(text_or_texts, str):
        texts = [text_or_texts]
    else:
        texts = list(text_or_texts)

    if not texts:
        logger.warning("parallel_process_text() called with no input text.")
        return []

    # Load rules if provided
    if rules_path:
        if not os.path.exists(rules_path):
            raise FileNotFoundError(f"Rules file not found: {rules_path}")
        rules = load_rules(rules_path)
        logger.info("Loaded %d rules from %s", len(rules), rules_path)
    else:
        logger.warning("No rules.json provided — returning raw chunks without scoring.")
        return _make_items_from_texts(texts, group_size)

    # Prepare DB if saving
    if save and storage is None:
        db_path = get_env("DB_PATH", "checks.db")
        storage = Storage(db_path=db_path)
        logger.info("Storage created at %s", db_path)

    # Convert to chunk items
    items = _make_items_from_texts(texts, group_size=group_size)

    # Run scoring step
    checker = Checker(rules=rules, storage=storage, max_workers=max_workers)
    results = checker.run_checks(items, save=save)

    logger.info("Parallel scoring completed: %d items processed.", len(results))
    return results


# ------------------------------------------------------------
# Pipeline for loading + processing entire folder of .txt files
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
    Load all text files from a folder → break → score.
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
            logger.exception("Failed to read file %s: %s", fpath, e)

    logger.info("Loaded %d text files from folder: %s", len(texts), folder_path)

    return parallel_process_text(
        texts,
        group_size=group_size,
        rules_path=rules_path,
        storage=storage,
        max_workers=max_workers,
        save=save
    )


# ------------------------------------------------------------
# Direct CLI demo (optional)
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
        print("Please provide --text-file or --folder")
        results = []

    print(f"Processed {len(results)} chunks.")
    if results:
        print("Sample output:", results[0])
