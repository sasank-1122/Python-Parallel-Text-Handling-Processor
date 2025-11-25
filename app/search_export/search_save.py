# app/search_export/search_save2.py
import pandas as pd
import csv
import re
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

def search_in_storage(storage, query: str, limit: int = 1000, use_regex: bool = False) -> List[Dict[str, Any]]:
    if not query:
        logger.warning("Empty query provided")
        return []
    rows = storage.query_checks(limit=limit)
    matched = []
    if use_regex:
        try:
            patt = re.compile(query, re.IGNORECASE)
        except re.error as e:
            logger.error("Invalid regex '%s': %s", query, e)
            return []
    else:
        qlower = query.lower()
    for r in rows:
        txt = (r.get("text") or "").lower()
        uid = (r.get("uid") or "").lower()
        if use_regex:
            if patt.search(txt) or patt.search(uid):
                matched.append(r)
        else:
            if qlower in txt or qlower in uid:
                matched.append(r)
    logger.info("Found %d matches for query '%s'", len(matched), query)
    return matched

def search_by_score(storage, min_score: Optional[float] = None, max_score: Optional[float] = None, limit: int = 1000) -> List[Dict[str, Any]]:
    return storage.query_checks(min_score=min_score, max_score=max_score, limit=limit)

def save_to_csv(rows: List[Dict[str, Any]], out_path: str) -> str:
    if not rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "uid", "score", "details", "ts", "text"])
        logger.warning("No rows exported; created empty CSV at %s", out_path)
        return out_path
    df = pd.DataFrame([{
        "id": r.get("id"),
        "uid": r.get("uid"),
        "score": r.get("score"),
        "details": str(r.get("details")),
        "ts": r.get("ts"),
        "text": (r.get("text") or "").replace("\n", " ").replace("\r", " ")
    } for r in rows])
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info("Saved %d rows to %s", len(rows), out_path)
    return out_path
