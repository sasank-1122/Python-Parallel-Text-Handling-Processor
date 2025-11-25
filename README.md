# 🚀 **Python Parallel Text Handling Processor**

A high-performance, scalable, multi-threaded **Text Processing, Rule-Based Scoring, Search, Storage & Analytics System** built entirely in Python.

This project is designed for **fast batch processing**, **parallel text chunking**, **rule-based compliance scoring**, **searchable history**, **email summaries**, and an **interactive Streamlit dashboard** — all optimized for large-scale text workflows.

---

## 📌 **Features at a Glance**

* ⚡ **Parallel Text Chunking** (Multi-threaded)
* 📚 **Rule-Based Checker & Scoring Engine**
* 🧠 **Storage Improver** — Auto-suggests new rules
* 🗃️ **SQLite Storage with Hash-level Deduplication**
* 🔍 **Powerful Search Engine (regex & keyword)**
* 📤 **CSV Exporter & Email Summaries**
* 📊 **Analytics Dashboard (Streamlit UI)**
* 📝 **PDF Reporting**
* 📁 **Full Folder Pipeline Support**
* 🛠 **Extensible architecture — Plug-and-Play Modules**

---

# 📂 **Project Folder Structure**

```
project/
│── app/
│   ├── checker/
│   │   ├── checker.py
│   │   └── rules.py
│   ├── storage/
│   │   ├── storage.py
│   │   └── storage_improver.py
│   ├── search_export/
│   │   ├── search_save.py
│   │   └── emailer.py
│   ├── text_processing/
│   │   ├── text_breaker.py
│   │   ├── text_loader.py
│   │   └── parallel_break_loader.py
│   ├── utils.py
│
│── data/
│── output/
│── improver_output/
│── streamlit_app.py
│── run.py
│── .env
│── .gitignore
│── README.md
```

---

# 🧠 **Project Overview**

This system processes large volumes of text using **parallel execution**, breaks them into **word-based chunks**, runs them through a **rule-based scoring engine**, stores each chunk with metadata, and exposes:

👉 A complete **end-to-end automated pipeline** via `run.py`
👉 A full **interactive GUI dashboard** via `streamlit_app.py`

The system is made for **linguistic analysis**, **text compliance auditing**, **sentiment scoring**, and **large-scale text mining**.

---

# 🏗 **System Architecture**

### **1️⃣ Text Ingestion**

* Loads `.txt` files individually or as a folder batch
* Cleans text (whitespace normalization)

### **2️⃣ Parallel Chunking**

Implemented in `parallel_break_loader.py`

* Breaks text by **word groups**
* Assigns UIDs
* Computes SHA-256 hash for deduplication

### **3️⃣ Rule-Based Scoring Engine**

`checker.py` + `rules.py`

* Loads `rules.json`
* Applies regex/keyword rules
* Computes score
* Stores rule hits ("details")

### **4️⃣ Storage Layer**

`storage.py`

* SQLite database
* Stores chunks, scores, timestamps
* Prevents duplicates based on text hash
* Provides fast querying

### **5️⃣ Storage Improver**

`storage_improver.py`

* Auto-suggests new rules
* Finds frequent repeated patterns
* Helps improve rule quality

### **6️⃣ Search & Export**

`search_save.py`

* Keyword/regex search
* Save results to CSV

### **7️⃣ Email Summary (Optional)**

`emailer.py`

* Builds automated email summaries
* Sends alerts if scoring crosses threshold

### **8️⃣ Streamlit UI Dashboard**

`streamlit_app.py`

* File upload & management
* Pipeline execution
* Record browser
* Analytics
* Rule manager
* PDF reporting
* Storage improver UI

---

# 🚀 **End-to-End Pipeline Workflow**

When you run:

```
python run.py
```

The system automatically performs:

1️⃣ Load rules from `rules.json` <br>
2️⃣ Load & clean text files <br>
3️⃣ Chunk into groups <br>
4️⃣ Deduplicate using SHA-256 hashes <br>
5️⃣ Run parallel rule-based scoring <br>
6️⃣ Store results in SQLite DB <br>
7️⃣ Run storage improver <br>
8️⃣ Perform sample search <br>
9️⃣ Export results to CSV <br>
🔟 Generate email summary <br>

---

# 🖥️ **Using the Streamlit Dashboard**

Run:

```
streamlit run streamlit_app.py
```

### The dashboard provides:

* File Upload Manager
* Pipeline Runner
* Overview Metrics
* Search & Filtering
* Chunk Browser
* Score Analytics
* Wordcloud
* Rule Hits Chart
* Storage Improver
* Rules.json Editor
* PDF Report Builder

---

# 🧩 **Key Modules Explained**

### 1. **parallel_break_loader.py**

Handles full pipeline:

* Chunking
* Deduplication
* Scoring
* Saving
* Parallel execution

### 2. **text_breaker.py**

* Cleans text
* Splits into fixed-size word groups

### 3. **checker.py**

* Applies rules
* Scores text
* Stores results

### 4. **storage.py**

* SQLite backend
* Query builder
* Hash existence check

### 5. **search_save.py**

* Regex / keyword search
* CSV export

### 6. **emailer.py**

* Email summary
* HTML email generator

### 7. **storage_improver.py**

* Auto-rule suggestions based on DB frequency

---

# ⚙️ **Installation & Setup**

### 1. Clone the repo

```bash
git clone https://github.com/springboardmentor587-star/Python-Parallel-Text-Handling-Processor.git
cd Python-Parallel-Text-Handling-Processor
```

### **2. Create & Activate Virtual Environment**

```
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
```

### **3. Install Dependencies**

```
pip install -r requirements.txt
```

### **4. Environment Variables**

Create `.env` file:

```
DB_PATH=checks.db
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
EMAIL_ADDRESS=youremail@gmail.com
EMAIL_PASSWORD=yourapppassword
```

### **5. Folder Setup**

```
mkdir data/support_text_files
mkdir output
mkdir improver_output
```

---

# 🧪 **Running the Pipeline**

```
python run.py
```

# 🖥️ **Running the Streamlit App**

```
streamlit run streamlit_app.py
```

---

# 📊 **Sample Outputs**

* Processed chunks
* Scores
* Applied rule IDs
* CSV exports
* PDF Reports
* Suggested rules
* Email summary

---

# 🤖 **Storage Improver (Rule Auto-Generator)**

AI-like rule analyzer that:

* scans high-frequency words/phrases
* detects missing rules
* suggests new rule patterns

Output → JSON suggestions saved in:

```
improver_output/suggestions.json
```

---

# 📧 **Email Summary**

Automatically compiles:

* recent scores
* high severity alerts
* rule hit summary

You can enable/disable email sending in `run.py`.

---

# 🛡 **Deduplication Logic**

Before scoring, each chunk is hashed:

```
sha256(text)
```

If hash already exists in DB → **skipped**.
This saves compute and prevents duplicates.

---

# 🧱 **Tech Stack**

| Component     | Technology               |
| ------------- | ------------------------ |
| Language      | Python                   |
| DB            | SQLite                   |
| UI            | Streamlit                |
| Parallelism   | ThreadPoolExecutor       |
| Email         | SMTP                     |
| Reports       | ReportLab                |
| Visualization | Plotly, WordCloud        |
| Logging       | Python Logging Framework |

---

# 📌 **Future Enhancements**

* Add ML-based scoring (BERT, spaCy)
* Real-time monitoring dashboard
* API layer (FastAPI)
* Vector search with embeddings
* Rule auto-learning (machine learning)
* Docker deployment

---

## 👥 Contributors

### 👨‍💼 Project Lead  
- **Charan Teja Mangali** — Lead Developer, System Architect & Mentor

### 🎓 Student Contributors  
- **Student Name 1** —   
- **Student Name 2** —   
- **Student Name 3** —   
- **Student Name 4** —    

---

# ⭐ **Support the Project**

If you like this project, consider giving it a ⭐ on GitHub!

---
