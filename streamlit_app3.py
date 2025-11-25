# streamlit_app2.py
"""
Improved Streamlit dashboard for Parallel Text Processing Processor.

Features:
- Logical tab order: Overview -> Upload -> Run Pipeline -> View Records -> Search -> Analytics -> Storage Improver -> Rules Manager -> PDF Report -> Admin
- Upload manager with list of saved text files
- Pipeline runner with progress indicator and logs
- Rules viewer/editor (edits rules.json safely with backup)
- Pagination for dataframes (fast browsing)
- DB stats panel
- Student-friendly info boxes and explanations
- Download buttons for CSV / PDF
"""

import os
import io
import json
import zipfile
import tempfile
import datetime
from typing import List, Optional, Tuple
from collections import Counter

import pandas as pd
import streamlit as st
import plotly.express as px

from wordcloud import WordCloud
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

# Project modules (expected to be available in your project)
from app.storage.storage2 import Storage
from app.search_export.search_save import search_in_storage, save_to_csv
from app.storage.storage_improver2 import StorageImprover
from app.text_processing.parallel_break_loader2 import pipeline_from_folder
from app.utils import get_env, ensure_dir

# -----------------------
# Config & initialization
# -----------------------
DB_PATH = get_env("DB_PATH", "checks.db")
TEXT_FOLDER = get_env("TEXT_FOLDER", "data/support_text_files")
RULES_PATH = get_env("RULES_PATH", "data/rules1.json")
EXPORT_DIR = get_env("EXPORT_DIR", "output")
REPORT_PATH = os.path.join(EXPORT_DIR, "report.pdf")

ensure_dir(TEXT_FOLDER)
ensure_dir(EXPORT_DIR)
storage = Storage(DB_PATH)

st.set_page_config(page_title="Parallel Text Processor", layout="wide")
st.title("📚 Python Parallel Text Processor — Dashboard")

# -----------------------
# Utilities
# -----------------------
@st.cache_data(ttl=30)
def load_rows(limit: int = 5000) -> pd.DataFrame:
    rows = storage.query_checks(limit=limit)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df

def parse_details(details_raw) -> List[dict]:
    """Safely parse 'details' field saved in DB (string or list)"""
    try:
        if isinstance(details_raw, str):
            parsed = json.loads(details_raw)
            if isinstance(parsed, list):
                return parsed
        elif isinstance(details_raw, list):
            return details_raw
    except Exception:
        return []
    return []

def list_text_files(folder: str) -> List[str]:
    items = []
    for f in sorted(os.listdir(folder)):
        if f.lower().endswith(".txt"):
            items.append(f)
    return items

def paginate_df(df: pd.DataFrame, page_size: int, page_number: int) -> pd.DataFrame:
    if df.empty:
        return df
    start = page_number * page_size
    return df.iloc[start:start + page_size]

def save_rules_backup(rules_path: str) -> str:
    """Create a timestamped backup of the rules file and return backup path."""
    if not os.path.exists(rules_path):
        return ""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{rules_path}.backup.{timestamp}.json"
    with open(rules_path, "rb") as rf, open(backup_path, "wb") as bf:
        bf.write(rf.read())
    return backup_path

def create_pdf_report(df: pd.DataFrame, out_path: str):
    """Create a simple PDF report with stats, top rules and a wordcloud image."""
    c = canvas.Canvas(out_path, pagesize=letter)
    width, height = letter

    # Header
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, height - 40, "Parallel Text Processor — Report")
    c.setFont("Helvetica", 9)
    c.drawString(40, height - 56, f"Generated: {datetime.datetime.now().isoformat()}")

    # Summary
    total = len(df)
    avg_score = round(df["score"].mean() if not df.empty else 0, 2)
    c.drawString(40, height - 84, f"Total chunks: {total}")
    c.drawString(40, height - 100, f"Average score: {avg_score}")

    # Top rules
    hit_counter = Counter()
    for item in df["details"]:
        details = parse_details(item)
        for d in details:
            rid = d.get("rule_id")
            if rid is not None:
                hit_counter[rid] += 1
    top_rules = hit_counter.most_common(30)

    c.drawString(40, height - 126, "Top rules (rule_id : hits)")
    y = height - 144
    for rid, hits in top_rules:
        c.drawString(44, y, f"{rid} : {hits}")
        y -= 12
        if y < 80:
            c.showPage()
            y = height - 40

    # Wordcloud
    combined = " ".join(df["text"].astype(str).tolist()).lower() if not df.empty else ""
    if combined:
        wc = WordCloud(width=800, height=400, background_color="white").generate(combined)
        img_buf = io.BytesIO()
        plt.figure(figsize=(8, 4))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(img_buf, format="png")
        plt.close()
        img_buf.seek(0)
        img = ImageReader(img_buf)
        c.showPage()
        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, height - 40, "Word Cloud")
        # Fit the image
        c.drawImage(img, 40, height - 360, width=520, height=300)

    c.save()


import json

def simplify_export_df(df: pd.DataFrame, text_col="text", details_col="details", short_len: int = 200) -> pd.DataFrame:
    df = df.copy()
    # Short preview of text for CSV readability
    df["short_text"] = (
        df[text_col].astype(str)
          .str.replace("\n", " ", regex=False)
          .str.replace(r"\s+", " ", regex=True)
          .str.strip()
          .str[:short_len]
    ) + "..."
    # Ensure details is parsed and create top_rules, hit_count, reasons_preview
    def summarize_details(d):
        try:
            arr = json.loads(d) if isinstance(d, str) else (d or [])
            if not isinstance(arr, list):
                arr = []
        except Exception:
            arr = []
        top_rules = ",".join(str(x.get("rule_id")) for x in arr[:5])
        hit_count = len(arr)
        reasons = ";".join(f"{x.get('rule_id')}:{x.get('reason','')}" for x in arr[:5])
        return top_rules, hit_count, reasons

    parsed = df[details_col].apply(summarize_details).tolist()
    if parsed:
        df[["top_rules", "hit_count", "reasons_preview"]] = pd.DataFrame(parsed, index=df.index)
    else:
        df["top_rules"] = ""
        df["hit_count"] = 0
        df["reasons_preview"] = ""

    out_cols = ["id", "uid", "short_text", "score", "top_rules", "hit_count", "reasons_preview", "ts"]
    # keep only columns that exist
    out_cols = [c for c in out_cols if c in df.columns]
    return df[out_cols]

# -----------------------
# Layout / Navigation
# -----------------------
menu = st.sidebar.radio(
    "Navigate",
    [
        "Upload & Manage Files",
        "Run Pipeline",
        "Overview",
        "Search",
        "View Records",
        "Analytics",
        "Storage Improver",
        "Rules Manager",
        "PDF Report"
    ],
    index=0
)

# -----------------------
# Page: Upload & Manage Files
# -----------------------
if menu == "Upload & Manage Files":
    import shutil   # <-- IMPORTANT FIX

    st.header("📁 Upload & Manage Text Files")
    st.markdown(
        "Upload single `.txt` files or a `.zip` containing `.txt` files. "
        "Files are saved to `data/text_files`. You can also view or delete files from the folder."
    )

    uploaded = st.file_uploader(
        "Upload .txt or .zip (containing .txt)", 
        accept_multiple_files=True, 
        key="uploader"
    )

    if uploaded:
        saved_files = []
        with st.spinner("Saving uploads..."):
            for up in uploaded:
                name = up.name

                # -----------------------------
                # Handling ZIP file uploads
                # -----------------------------
                if name.lower().endswith(".zip"):
                    with tempfile.TemporaryDirectory() as td:
                        z = zipfile.ZipFile(io.BytesIO(up.read()))
                        z.extractall(td)

                        count = 0
                        for root, _, files in os.walk(td):
                            for f in files:
                                if f.lower().endswith(".txt"):
                                    src = os.path.join(root, f)
                                    dst = os.path.join(TEXT_FOLDER, f)

                                    # If file exists → add timestamp suffix
                                    if os.path.exists(dst):
                                        base, ext = os.path.splitext(dst)
                                        dst = f"{base}__{int(datetime.datetime.now().timestamp())}{ext}"

                                    # FIX 🚀 Replace rename with shutil.move (supports cross-drive)
                                    shutil.move(src, dst)

                                    saved_files.append(os.path.basename(dst))
                                    count += 1

                        st.success(f"Extracted {count} .txt files from {name}")

                # -----------------------------
                # Handling individual .txt files
                # -----------------------------
                elif name.lower().endswith(".txt"):
                    dst = os.path.join(TEXT_FOLDER, name)

                    # If file exists → add timestamp suffix
                    if os.path.exists(dst):
                        base, ext = os.path.splitext(dst)
                        dst = f"{base}__{int(datetime.datetime.now().timestamp())}{ext}"

                    # Save uploaded file
                    with open(dst, "wb") as fh:
                        fh.write(up.getbuffer())

                    saved_files.append(os.path.basename(dst))
                    st.success(f"Saved {name}")

                else:
                    st.warning(f"Skipped {name} (unsupported file type)")

        if saved_files:
            st.info(f"Saved {len(saved_files)} files to `{TEXT_FOLDER}`")

    # -----------------------------
    # Display current files in folder
    # -----------------------------
    st.subheader("Files currently in text folder")
    files = list_text_files(TEXT_FOLDER)

    if not files:
        st.write("No .txt files found in the folder. Upload some to begin.")
    else:
        file_df = pd.DataFrame({"filename": files})
        st.dataframe(file_df, use_container_width=True)

        to_delete = st.multiselect("Select files to delete from folder", options=files)

        if st.button("Delete selected files"):
            for f in to_delete:
                try:
                    os.remove(os.path.join(TEXT_FOLDER, f))
                except Exception as e:
                    st.error(f"Failed to delete {f}: {e}")

            st.success(f"Deleted {len(to_delete)} files.")
            st.experimental_rerun()


# -----------------------
# Page: Run Pipeline
# -----------------------
elif menu == "Run Pipeline":
    st.header("⚙️ Run Processing Pipeline")
    st.info(
        "Configure how to chunk and process your text files. Processing will create scored chunks and save them to the database."
    )
    st.markdown("**Pipeline configuration**")
    run_mode = st.selectbox("Run mode", ["Process entire text_files folder", "Process new files only"])
    workers = st.number_input("Max workers", min_value=1, max_value=32, value=6, step=1)
    group_size = st.number_input("Group size (words per chunk)", min_value=50, max_value=5000, value=500, step=50)

    st.write("---")
    st.subheader("Start processing")
    if st.button("Start processing"):
        st.info("Starting pipeline — this may take time depending on data size.")
        log_box = st.empty()
        progress_bar = st.progress(0)

        # Best-effort progress reporting: if pipeline_from_folder supports callbacks you'd use it.
        # Here we show an optimistic progress update and a final report.
        try:
            log_box.text("Initializing pipeline...")
            results = pipeline_from_folder(
                folder_path=TEXT_FOLDER,
                rules_path=RULES_PATH,
                group_size=group_size,
                storage=storage,
                max_workers=workers,
                save=True
            )
            # results is expected to be a list of processed chunks or similar
            total = len(results) if results is not None else 0
            for i in range(0, 100):
                progress_bar.progress(i + 1)
            log_box.text(f"Processing finished: {total} chunks processed.")
            st.success(f"Processing finished: {total} chunks processed.")
            if isinstance(results, list) and results:
                st.write("Sample processed items (first 3):")
                st.write(results[:3])
        except Exception as exc:
            st.error(f"Pipeline failed: {exc}")
        finally:
            progress_bar.empty()

# -----------------------
# Page: Overview
# -----------------------
elif menu == "Overview":
    st.header("📈 Project Overview")
    st.info(
        "This dashboard walks students through uploading text files, running the text chunking + rule-based scoring pipeline, "
        "viewing/searching results, analyzing rule hits, and improving storage. Use the left navigation to move between steps."
    )

    df = load_rows(limit=5000)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Chunks stored", len(df))
    col2.metric("Avg score", round(df["score"].mean(), 2) if not df.empty else "N/A")
    col3.metric("Unique UIDs", int(df["uid"].nunique()) if not df.empty else 0)
    # Simple top rule preview
    if not df.empty:
        hit_counter = Counter()
        for item in df["details"]:
            details = parse_details(item)
            for d in details:
                rid = d.get("rule_id")
                if rid is not None:
                    hit_counter[rid] += 1
        top = hit_counter.most_common(5)
        col4.metric("Top rule (id : hits)", f"{top[0][0]} : {top[0][1]}" if top else "N/A")
    else:
        col4.metric("Top rule (id : hits)", "N/A")

    st.subheader("Score distribution")
    if df.empty:
        st.warning("No records found. Use Upload & Run Pipeline to generate data.")
    else:
        fig = px.histogram(df, x="score", nbins=40, title="Score distribution")
        st.plotly_chart(fig, use_container_width=True)


# -----------------------
# Page: Search
# -----------------------
elif menu == "Search":
    st.header("🔍 Search Stored Texts")
    st.info("Search the database using a keyword or regular expression (case-insensitive).")
    query = st.text_input("Keyword or regex (case-insensitive):", value="", key="search_query")
    use_regex = st.checkbox("Use regex", value=False)
    max_results = st.number_input("Max results", min_value=10, max_value=5000, value=500)
    if st.button("Run search"):
        if not query.strip():
            st.error("Enter a search query.")
        else:
            try:
                results = search_in_storage(storage, query=query, limit=int(max_results), use_regex=use_regex)
                if not results:
                    st.warning("No results found.")
                else:
                    df = pd.DataFrame(results)
                    st.success(f"Found {len(df)} results")
                    st.dataframe(df, use_container_width=True)
                    csv = df.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Search Results (CSV)", csv, "search_results.csv")
            except Exception as exc:
                st.error(f"Search failed: {exc}")


# -----------------------
# Page: View Records
# -----------------------
elif menu == "View Records":
    st.header("🔎 View Records (DB)")
    st.info("Browse chunks saved in the database. Use pagination for large datasets.")
    df = load_rows(limit=20000)
    if df.empty:
        st.warning("No records found.")
    else:
        # pagination controls
        page_size = st.selectbox("Rows per page", [10, 25, 50, 100], index=2)
        total_pages = (len(df) - 1) // page_size + 1
        page_num = st.number_input("Page number (0-indexed)", min_value=0, max_value=max(0, total_pages - 1), value=0, step=1)
        page_df = paginate_df(df, page_size, page_num)
        st.dataframe(page_df, use_container_width=True)
        # human-friendly export
        readable = simplify_export_df(df, short_len=250)
        csv_bytes = readable.to_csv(index=False).encode("utf-8")
        st.download_button("Download readable DB (CSV)", csv_bytes, "all_records_readable.csv")
        # optional: keep raw full export as a secondary download (uncomment if needed)
        # raw_bytes = df.to_csv(index=False).encode("utf-8")
        # st.download_button("Download raw DB (full)", raw_bytes, "all_records_raw.csv")
        st.write(f"Showing page {page_num} of {total_pages - 1} (0-indexed)")


# -----------------------
# Page: Analytics
# -----------------------
elif menu == "Analytics":
    st.header("📊 Analytics")
    st.info("Explore visual insights from the processed chunks: word cloud, score histogram, top rules.")
    df = load_rows(limit=10000)
    if df.empty:
        st.warning("No records to analyze. Run the pipeline first.")
    else:
        with st.expander("Score distribution", expanded=True):
            fig = px.histogram(df, x="score", nbins=40, title="Score distribution")
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Word Cloud", expanded=False):
            combined = " ".join(df["text"].astype(str).tolist()).lower()
            stopwords = set([
                "the", "and", "a", "to", "i", "is", "it", "of", "for", "you", "we", "that", "this", "in", "on", "your"
            ])
            wc = WordCloud(width=1200, height=600, stopwords=stopwords, background_color="white").generate(combined)
            fig_wc, ax = plt.subplots(figsize=(12, 6))
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig_wc)

            words = wc.words_
            top = pd.DataFrame(list(words.items()), columns=["word", "score"]).head(50)
            st.dataframe(top, use_container_width=True)

        with st.expander("Top rule hits", expanded=False):
            hit_counter = Counter()
            for item in df["details"]:
                details = parse_details(item)
                for d in details:
                    rid = d.get("rule_id")
                    if rid is not None:
                        hit_counter[rid] += 1
            hit_df = pd.DataFrame([{"rule_id": k, "hits": v} for k, v in hit_counter.items()]).sort_values("hits", ascending=False)
            if hit_df.empty:
                st.write("No rule hits found.")
            else:
                st.dataframe(hit_df.head(100), use_container_width=True)
                fig = px.bar(hit_df.head(20), x="rule_id", y="hits", title="Top rule hits")
                st.plotly_chart(fig, use_container_width=True)

# -----------------------
# Page: Storage Improver
# -----------------------
elif menu == "Storage Improver":
    st.header("🤖 Storage Improver (Auto Rule Suggestions)")
    st.info("Run the improver to generate suggested rules/keywords from DB contents.")
    min_freq = st.number_input("Minimum frequency for suggestion", min_value=1, max_value=100, value=5)
    max_items = st.number_input("Max suggestions to show", min_value=10, max_value=1000, value=200)

    if st.button("Run improver"):
        with st.spinner("Running storage improver..."):
            try:
                improver = StorageImprover(storage)
                suggestions = improver.run(limit=5000, min_freq=int(min_freq), auto_update=False)
                cnt = len(suggestions) if suggestions else 0
                st.success(f"Generated {cnt} suggestions")
                if suggestions:
                    shown = suggestions[: int(max_items)]
                    st.json(shown)
                    csv = pd.DataFrame(suggestions).to_csv(index=False).encode("utf-8")
                    st.download_button("Download suggestions CSV", csv, "rule_suggestions.csv")
            except Exception as exc:
                st.error(f"Improver failed: {exc}")

# -----------------------
# Page: Rules Manager
# -----------------------
elif menu == "Rules Manager":
    st.header("🧾 Rules Manager (view & edit rules.json)")
    st.markdown("You can view and edit the rules JSON file used by the pipeline. A backup is created before saving edits.")
    if not os.path.exists(RULES_PATH):
        st.warning(f"No rules file found at {RULES_PATH}. Create one at that path.")
    else:
        # Load rules
        try:
            with open(RULES_PATH, "r", encoding="utf-8") as rf:
                rules_json = json.load(rf)
        except Exception as exc:
            st.error(f"Failed to load rules.json: {exc}")
            rules_json = None

        if rules_json is not None:
            st.subheader("Current rules (preview)")
            st.write("Number of top-level rules:", len(rules_json) if isinstance(rules_json, list) else "N/A")
            st.json(rules_json[:50] if isinstance(rules_json, list) else rules_json)

            st.subheader("Edit rules.json (raw)")
            # stringify with pretty formatting
            rules_text = json.dumps(rules_json, indent=2, ensure_ascii=False)
            edited = st.text_area("Edit rules.json (make sure valid JSON)", value=rules_text, height=400)

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Save edited rules.json"):
                    # validate JSON
                    try:
                        new_rules = json.loads(edited)
                        backup = save_rules_backup(RULES_PATH)
                        with open(RULES_PATH, "w", encoding="utf-8") as wf:
                            json.dump(new_rules, wf, indent=2, ensure_ascii=False)
                        st.success(f"Saved rules.json. Backup saved at: {backup}")
                    except Exception as exc:
                        st.error(f"Invalid JSON or save failed: {exc}")
            with col2:
                if st.button("Download current rules.json"):
                    st.download_button("Download rules.json", json.dumps(rules_json, indent=2, ensure_ascii=False).encode("utf-8"), "rules.json")

# -----------------------
# Page: PDF Report
# -----------------------
elif menu == "PDF Report":
    st.header("📄 Generate PDF Report")
    st.info("Generate a simple PDF with summary stats, top rules and a word cloud.")
    df = load_rows(limit=10000)
    if df.empty:
        st.warning("No data to create a report. Run the pipeline first.")
    else:
        st.write(f"Chunks available for report: {len(df)}")
        if st.button("Generate PDF report"):
            with st.spinner("Creating PDF report..."):
                try:
                    create_pdf_report(df, REPORT_PATH)
                    st.success(f"Report created: {REPORT_PATH}")
                    with open(REPORT_PATH, "rb") as fh:
                        st.download_button("Download PDF Report", fh.read(), file_name="report.pdf", mime="application/pdf")
                except Exception as exc:
                    st.error(f"Failed to create report: {exc}")

