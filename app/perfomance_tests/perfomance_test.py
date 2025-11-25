"""
app/perfomance_tests/perfomance_test.py

Performance Benchmark for the Parallel Text Processing Pipeline

This script measures:
    ✔ File loading speed
    ✔ Text chunking performance
    ✔ Parallel scoring speed (multiprocessing)
    ✔ CPU usage and RAM consumption
    ✔ Full pipeline execution time

Outputs:
    - Detailed console logs
    - performance_report.txt inside performance_tests/reports/
"""

import os
import time
import psutil
import cProfile
import pstats
from multiprocessing import Pool, cpu_count

from text_processing.text_breaker import break_text_into_groups
from checker.rules import load_rules
from checker.checker import Checker
from storage.storage import Storage
from app.utils import ensure_dir


# ------------------------------------------------------------
# 1. LAZY LOADER — Reads huge files safely
# ------------------------------------------------------------
def read_large_file(file_path, lines_limit=None):
    """Generator that streams large files line-by-line."""
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if lines_limit and i >= lines_limit:
                break
            yield line.strip()


# ------------------------------------------------------------
# 2. PERFORMANCE CHUNKING
# ------------------------------------------------------------
def chunk_text(text, chunk_size=5000):
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]


# ------------------------------------------------------------
# 3. SIMPLE ANALYSIS FUNCTION (CPU benchmark)
# ------------------------------------------------------------
def analyze_chunk(chunk):
    """Very lightweight CPU-based text analysis."""
    words = chunk.split()
    chars = len(chunk)
    return {"words": len(words), "chars": chars}


def run_parallel_cpu_benchmark(chunks):
    """Tests multiprocessing CPU performance."""
    start = time.time()

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(analyze_chunk, chunks)

    end = time.time()
    print(f"Parallel CPU benchmark completed in {end - start:.2f} sec")
    return results, end - start


# ------------------------------------------------------------
# 4. PERFORMANCE PROFILER
# ------------------------------------------------------------
def profile_function(func, *args):
    """Profiles any function and saves the top 20 slowest calls."""
    ensure_dir("app/performance_tests/reports/")
    prof_file = "app/performance_tests/reports/profile.prof"
    txt_report = "app/performance_tests/reports/performance_report.txt"

    cProfile.runctx("func(*args)", globals(), locals(), prof_file)

    stats = pstats.Stats(prof_file)
    stats.sort_stats("time")

    with open(txt_report, "w") as f:
        stats.stream = f
        stats.print_stats(20)

    print(f"Performance profile saved: {txt_report}")


# ------------------------------------------------------------
# 5. SYSTEM MONITOR
# ------------------------------------------------------------
def monitor_resources():
    process = psutil.Process(os.getpid())
    memory = process.memory_info().rss / (1024 ** 2)
    cpu = psutil.cpu_percent(interval=1)
    return memory, cpu


# ------------------------------------------------------------
# 6. FULL PIPELINE PERFORMANCE TEST
# ------------------------------------------------------------
def run_full_pipeline_test():
    print("\n===== PERFORMANCE TEST STARTED =====\n")

    file_path = "data/large_text.txt"
    rules_path = "data/rules.json"

    if not os.path.exists(file_path):
        print("❌ large_text.txt not found in /data/")
        return

    if not os.path.exists(rules_path):
        print("❌ rules.json not found in /data/")
        return

    # Step 1 — Load rules
    rules = load_rules(rules_path)

    # Step 2 — Load text (streaming)
    print("Loading text from file...")
    text_data = "\n".join(list(read_large_file(file_path)))
    print(f"Loaded text length: {len(text_data):,} characters")

    # Step 3 — Chunk text
    print("\nSplitting text into chunks...")
    chunks = chunk_text(text_data, chunk_size=8000)
    print(f"Chunks created: {len(chunks)}")

    # Step 4 — Monitor usage BEFORE benchmark
    mem_before, cpu_before = monitor_resources()
    print(f"\nBefore Execution → RAM: {mem_before:.2f}MB | CPU: {cpu_before:.2f}%")

    # Step 5 — Benchmark parallel CPU work
    print("\nRunning CPU benchmark...")
    cpu_results, cpu_time = run_parallel_cpu_benchmark(chunks)

    # Step 6 — Rule checker speed test
    print("\nRunning rule scoring benchmark...")
    storage = Storage("performance_test.db")
    checker = Checker(rules, storage, max_workers=cpu_count())

    start = time.time()
    checker_input = [{"uid": f"{i}", "text": c} for i, c in enumerate(chunks)]
    checker.run_checks(checker_input, save=False)
    checker_time = time.time() - start

    # Step 7 — Monitor usage AFTER benchmark
    mem_after, cpu_after = monitor_resources()
    print(f"\nAfter Execution → RAM: {mem_after:.2f}MB | CPU: {cpu_after:.2f}%")

    # Step 8 — Save performance summary
    summary_path = "app/performance_tests/reports/performance_summary.txt"
    ensure_dir("app/performance_tests/reports/")

    with open(summary_path, "w") as f:
        f.write("=== PERFORMANCE SUMMARY ===\n")
        f.write(f"Total text size: {len(text_data):,} chars\n")
        f.write(f"Chunks created: {len(chunks)}\n")
        f.write(f"CPU test time: {cpu_time:.2f} sec\n")
        f.write(f"Rule scoring time: {checker_time:.2f} sec\n")
        f.write(f"RAM before: {mem_before:.2f} MB\n")
        f.write(f"RAM after: {mem_after:.2f} MB\n")
        f.write(f"RAM usage increased: {mem_after - mem_before:.2f} MB\n")
        f.write(f"CPU before: {cpu_before:.2f}%\n")
        f.write(f"CPU after: {cpu_after:.2f}%\n")

    print("\nPerformance summary saved:", summary_path)
    print("\n===== PERFORMANCE TEST COMPLETED =====")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
if __name__ == "__main__":
    run_full_pipeline_test()
