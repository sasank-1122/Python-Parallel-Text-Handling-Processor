"""
app/perfomance_tests/test_big_texts.py

Performance test for the FULL parallel pipeline:
    ✔ Reads a large text file
    ✔ Breaks into groups
    ✔ Runs rule-based scoring in parallel
    ✔ Reports execution time and chunk count

Use this to verify scaling on large inputs.
"""

import time
import os
from app.text_processing.parallel_break_loader import parallel_process_text
from app.utils import get_logger, get_env

logger = get_logger(__name__)


def test_large_text_file(
    file_path: str,
    rules_path: str = "data/rules.json",
    group_size: int = 800,
    workers: int = 6
):
    """
    Run pipeline performance test on a large text file.

    Args:
        file_path: Path to a large .txt file
        rules_path: Path to rules.json
        group_size: Words per chunk
        workers: Max parallel threads
    """

    if not os.path.exists(file_path):
        logger.error("Large text file not found: %s", file_path)
        return

    if not os.path.exists(rules_path):
        logger.error("Rules file not found: %s", rules_path)
        return

    logger.info("Loading large text file: %s", file_path)
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        big_text = f.read()

    logger.info("Text loaded. Length: %s characters", f"{len(big_text):,}")

    start = time.time()

    results = parallel_process_text(
        text_or_texts=big_text,
        group_size=group_size,
        rules_path=rules_path,
        max_workers=workers,
        save=False   # We only want speed test — skip DB write
    )

    end = time.time()
    duration = end - start

    logger.info("==== PERFORMANCE REPORT ====")
    logger.info("Chunks processed : %d", len(results))
    logger.info("Execution time   : %.2f seconds", duration)
    logger.info("Words per chunk  : %d", group_size)
    logger.info("Parallel workers : %d", workers)

    print("\n=== Big Text Test Complete ===")
    print(f"Chunks: {len(results)}")
    print(f"Time:   {duration:.2f} sec")
    if results:
        print("Sample output:", results[0])

    return {
        "chunks": len(results),
        "time": duration,
        "group_size": group_size,
        "workers": workers
    }


if __name__ == "__main__":
    # Values can be changed directly here or in environment variables
    file_path = get_env("BIG_TEXT_PATH", "data/large_text.txt")
    rules_path = get_env("RULES_PATH", "data/rules.json")
    group_size = int(get_env("GROUP_SIZE", 800))
    workers = int(get_env("WORKERS", 6))

    test_large_text_file(
        file_path=file_path,
        rules_path=rules_path,
        group_size=group_size,
        workers=workers
    )
