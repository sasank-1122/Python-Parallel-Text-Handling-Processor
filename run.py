"""
run.py
Direct End-to-End Workflow

When you run: python run.py
It will automatically perform:

1. Load rules.json
2. Load all .txt files from text_files/ folder
3. Break text into chunks
4. Run rule-based scoring in parallel
5. Save results to SQLite DB
6. Run storage improver (optional)
7. Run a sample search
8. Export results to CSV
9. Build summary email (sending optional)

Everything is automatic.
"""

import os
from app.utils import get_env, get_logger, ensure_dir
from app.storage.storage import Storage
from app.text_processing.parallel_break_loader import parallel_process_text, pipeline_from_folder
from app.search_export.search_save import search_in_storage, save_to_csv
from app.search_export.emailer import build_summary_email, send_email
from app.storage.storage_improver import StorageImprover

logger = get_logger("direct-run", level="INFO")

# -----------------------------
# CONFIGURATION
# -----------------------------
TEXT_FOLDER = "data/support_text_files/"
TEXT_FILE_PATH = None
RULES_PATH = "data/rules1.json"
DB_PATH = get_env("DB_PATH", "checks.db")
EXPORT_PATH = "output/search_export.csv"
SEND_EMAIL = True      # Change to True if you want real email sending


# 1. Ensure required folders
ensure_dir("output")
ensure_dir("improver_output")
ensure_dir(TEXT_FOLDER)


# 2. Load storage
storage = Storage(DB_PATH)
logger.info(f"Using database: {DB_PATH}")


# 3. Run full pipeline
def run_full_pipeline():
    logger.info("🚀 Starting Full Pipeline Execution...")

    if not os.path.exists(RULES_PATH):
        raise FileNotFoundError(f"Rules file not found: {RULES_PATH}")

    if not os.listdir(TEXT_FOLDER):
        raise FileNotFoundError(f"No text files found inside {TEXT_FOLDER}")

    results = pipeline_from_folder(
        folder_path=TEXT_FOLDER,
        rules_path=RULES_PATH,
        group_size=500,
        storage=storage,
        max_workers=6,
        save=True
    )

    logger.info(f"Completed processing. Total chunks processed: {len(results)}")

    if results:
        print("\nSample Output:", results[0])

    return results


# 4. Run improver
def run_improver():
    logger.info("🔧 Running Storage Improver...")
    improver = StorageImprover(storage)
    suggestions = improver.run(limit=500, min_freq=5, auto_update=False)

    ensure_dir("improver_output")
    import json
    with open("improver_output/suggestions.json", "w") as f:
        json.dump(suggestions, f, indent=4)

    logger.info(f"Improver generated {len(suggestions)} rule suggestions.")
    return suggestions


# 5. Search Example
def run_sample_search():
    logger.info("🔍 Running sample search: query='delay'")
    results = search_in_storage(storage, query="delay", limit=50)
    save_to_csv(results, EXPORT_PATH)
    logger.info(f"Search results saved to {EXPORT_PATH}")
    return results


# 6. Email Summary (Optional)
def run_email():
    logger.info("📧 Preparing summary email...")

    recent = storage.query_checks(limit=50)

    smtp = {
        "host": get_env("SMTP_SERVER"),
        "port": int(get_env("SMTP_PORT", 587)),
        "user": get_env("EMAIL_ADDRESS"),
        "password": get_env("EMAIL_PASSWORD"),
        "from": get_env("EMAIL_FROM", get_env("EMAIL_ADDRESS")),
        "to": get_env("EMAIL_TO", get_env("EMAIL_ADDRESS")),
    }

    msg = build_summary_email(
        check_results=recent,
        smtp_from=smtp["from"],
        smtp_to=smtp["to"],
        min_score_alert=-5
    )

    if SEND_EMAIL:
        send_email(
            msg,
            smtp["host"],
            smtp["port"],
            smtp["user"],
            smtp["password"]
        )
        logger.info("Email sent successfully.")
    else:
        logger.info("Email sending disabled (SEND_EMAIL=False).")

    return msg



# MAIN EXECUTION
if __name__ == "__main__":
    print("\n==================================")
    print("   PARALLEL TEXT PROCESSOR WORKGLOW STARTED")
    print("==================================")

    results = run_full_pipeline()
    suggestions = run_improver()
    search_output = run_sample_search()
    email_msg = run_email()

    print("\n🎉 WORKFLOW COMPLETED SUCCESSFULLY!")
