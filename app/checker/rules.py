# app/checker/rules.py
import json
import re
import logging
from typing import Dict, Any, Tuple, Optional, List

logger = logging.getLogger(__name__)

def load_rules(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if not isinstance(data, list):
                raise ValueError("Rules JSON must be a list of rule objects")
            return data
    except FileNotFoundError:
        logger.error("Rules file not found: %s", path)
        raise
    except json.JSONDecodeError as e:
        logger.error("Could not parse rules JSON: %s", e)
        raise

def evaluate_rule(rule: Dict[str, Any], text: Optional[str]) -> Tuple[float, Optional[str]]:
    t = (text or "")
    typ = rule.get("type")
    score = float(rule.get("score", 0) or 0)

    if typ == "keyword_any":
        kws = rule.get("keywords", []) or []
        lowered = t.lower()
        for kw in kws:
            if not kw:
                continue
            if kw.lower() in lowered:
                return score, f"found_keyword:{kw}"
        return 0, None

    if typ == "uppercase_ratio":
        letters = [c for c in t if c.isalpha()]
        if not letters:
            return 0, None
        ups = sum(1 for c in letters if c.isupper())
        ratio = ups / len(letters)
        try:
            thresh = float(rule.get("threshold", 1.0))
        except Exception:
            thresh = 1.0
        if ratio >= thresh:
            return score, f"uppercase_ratio:{ratio:.2f}"
        return 0, None

    if typ == "length_min":
        try:
            min_chars = int(rule.get("min_chars", 0))
        except Exception:
            min_chars = 0
        if len(t) >= min_chars:
            return score, f"length:{len(t)}"
        return 0, None

    if typ == "regex_match":
        pattern = rule.get("pattern")
        if not pattern:
            return 0, None
        try:
            if re.search(pattern, t, flags=re.IGNORECASE):
                return score, f"regex_match:{pattern}"
        except re.error as e:
            logger.error("Invalid regex in rule %s: %s", rule.get("id"), e)
        return 0, None

    if typ == "contains_phrase":
        phrase = (rule.get("phrase") or "").lower()
        if phrase and phrase in t.lower():
            return score, f"found_phrase:{phrase}"
        return 0, None

    if typ == "word_count_min":
        try:
            min_words = int(rule.get("min_words", 0))
        except Exception:
            min_words = 0
        if len(t.split()) >= min_words:
            return score, f"word_count:{len(t.split())}"
        return 0, None

    if typ == "starts_with":
        prefix = rule.get("prefix", "")
        if prefix and t.startswith(prefix):
            return score, f"starts_with:{prefix}"
        return 0, None

    if typ == "ends_with":
        suffix = rule.get("suffix", "")
        if suffix and t.endswith(suffix):
            return score, f"ends_with:{suffix}"
        return 0, None

    if typ == "not_contains":
        word = (rule.get("word") or "").lower()
        if word and word not in t.lower():
            return score, f"not_contains:{word}"
        return 0, None

    logger.warning("Unknown rule type: %s", typ)
    return 0, None
