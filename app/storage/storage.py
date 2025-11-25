# app/storage/storage2.py
import sqlite3
import json
from contextlib import closing
import logging
from typing import Optional, List, Dict, Any

from app.utils import compute_text_hash   # <-- IMPORTANT NEW IMPORT

logger = logging.getLogger(__name__)

class Storage:
    def __init__(self, db_path: str = "checks.db", timeout: float = 5.0):
        self.db_path = db_path
        self.timeout = timeout
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False, timeout=self.timeout)

    # -------------------------------------------------------------------
    # Initialize DB — now includes text_hash and auto-migration
    # -------------------------------------------------------------------
    def _init_db(self) -> None:
        with closing(self._conn()) as conn:
            c = conn.cursor()

            # Create original columns
            c.execute("""
                CREATE TABLE IF NOT EXISTS checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    uid TEXT,
                    text TEXT,
                    score REAL,
                    details TEXT,
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 🔥 NEW: Ensure text_hash column exists (auto-migration)
            try:
                c.execute("ALTER TABLE checks ADD COLUMN text_hash TEXT")
                logger.warning("Added missing text_hash column to checks table.")
            except Exception:
                pass  # column already exists

            # Indexes for speed
            c.execute("CREATE INDEX IF NOT EXISTS idx_checks_uid ON checks(uid)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_checks_score ON checks(score)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_checks_hash ON checks(text_hash)")

            conn.commit()
            logger.debug("DB initialized at %s", self.db_path)

    # -------------------------------------------------------------------
    # NEW: Check if a hash already exists (used for deduplication)
    # -------------------------------------------------------------------
    def exists_hash(self, text_hash: str) -> bool:
        with closing(self._conn()) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM checks WHERE text_hash = ? LIMIT 1", (text_hash,))
            row = c.fetchone()
            return row is not None

    # -------------------------------------------------------------------
    # Save a chunk with hash support
    # -------------------------------------------------------------------
    def save_check(self, uid: str, text: str, score: float, details: Any) -> None:
        # Compute hash for dedupe
        text_hash = compute_text_hash(text)

        # Serialize details
        try:
            details_json = json.dumps(details, ensure_ascii=False)
        except Exception:
            details_json = json.dumps(str(details), ensure_ascii=False)

        with closing(self._conn()) as conn:
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO checks (uid, text, score, details, text_hash)
                VALUES (?, ?, ?, ?, ?)
                """,
                (uid, text, float(score or 0.0), details_json, text_hash)
            )
            conn.commit()
            logger.debug("Saved check uid=%s score=%s", uid, score)

    # -------------------------------------------------------------------
    # Query records from DB
    # -------------------------------------------------------------------
    def query_checks(self, min_score: Optional[float] = None, max_score: Optional[float] = None, limit: int = 1000) -> List[Dict[str, Any]]:
        with closing(self._conn()) as conn:
            c = conn.cursor()
            q = "SELECT id, uid, text, score, details, ts, text_hash FROM checks WHERE 1=1"
            params = []

            if min_score is not None:
                q += " AND score >= ?"
                params.append(min_score)
            if max_score is not None:
                q += " AND score <= ?"
                params.append(max_score)

            q += " ORDER BY ts DESC LIMIT ?"
            params.append(limit)

            c.execute(q, params)
            rows = c.fetchall()

            results = []
            for r in rows:
                try:
                    details = json.loads(r[4]) if r[4] else None
                except Exception:
                    details = r[4]

                results.append({
                    'id': r[0],
                    'uid': r[1],
                    'text': r[2],
                    'score': r[3],
                    'details': details,
                    'ts': r[5],
                    'text_hash': r[6]
                })
            return results

    # -------------------------------------------------------------------
    # Fetch by UID
    # -------------------------------------------------------------------
    def get_check_by_uid(self, uid: str) -> Optional[Dict[str, Any]]:
        with closing(self._conn()) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT id, uid, text, score, details, ts, text_hash "
                "FROM checks WHERE uid = ? ORDER BY ts DESC LIMIT 1",
                (uid,)
            )
            r = c.fetchone()
            if not r:
                return None

            try:
                details = json.loads(r[4]) if r[4] else None
            except Exception:
                details = r[4]

            return {
                'id': r[0],
                'uid': r[1],
                'text': r[2],
                'score': r[3],
                'details': details,
                'ts': r[5],
                'text_hash': r[6]
            }

    # -------------------------------------------------------------------
    # Delete single record
    # -------------------------------------------------------------------
    def delete_check(self, uid: str) -> bool:
        with closing(self._conn()) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM checks WHERE uid = ?", (uid,))
            conn.commit()
            return c.rowcount > 0

    # -------------------------------------------------------------------
    # Clear DB
    # -------------------------------------------------------------------
    def clear_all(self) -> None:
        with closing(self._conn()) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM checks")
            conn.commit()
            logger.warning("Cleared all checks")
