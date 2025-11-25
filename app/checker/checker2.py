# app/checker/checker.py
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import logging
import json
import re

from .rules import evaluate_rule

logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# Helper: word count
# -----------------------------------------------------------
def count_words(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"\w+", text))


# -----------------------------------------------------------
# Main scoring with normalization
# -----------------------------------------------------------
def score_text(rules: List[Dict[str, Any]], item: Dict[str, Any]) -> Dict[str, Any]:
    text = item.get("text", "") or ""
    uid = item.get("uid")

    # Raw rule-based score
    raw_score = 0.0
    details = []

    for rule in rules:
        try:
            s, reason = evaluate_rule(rule, text)
            if s:
                raw_score += float(s)
                details.append({
                    'rule_id': rule.get('id'),
                    'score': float(s),
                    'reason': reason
                })
        except Exception as e:
            logger.exception("Error evaluating rule %s: %s", rule.get('id'), e)
            details.append({
                'rule_id': rule.get('id'),
                'score': 0.0,
                'reason': f"error:{e}"
            })

    # -------------------------------------------------------
    # NORMALIZATION STEP
    # score_per_100_words = raw_score / (word_count / 100)
    # -------------------------------------------------------
    wc = count_words(text)

    if wc > 0:
        normalized_score = raw_score / (wc / 100)
    else:
        normalized_score = raw_score  # fallback

    # Round for nicer UI output
    normalized_score = round(normalized_score, 3)

    return {
        'uid': uid,
        'text': text,
        'raw_score': raw_score,
        'score': normalized_score,      # <- final normalized score saved to DB
        'details': details,
        'word_count': wc
    }


# -----------------------------------------------------------
# Checker class
# -----------------------------------------------------------
class Checker:
    def __init__(self, rules: List[Dict[str, Any]], storage: Optional[Any] = None, max_workers: int = 6):
        self.rules = rules or []
        self.storage = storage
        self.max_workers = max_workers

    def run_checks(self, items: List[Dict[str, Any]], save: bool = True) -> List[Dict[str, Any]]:
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(score_text, self.rules, it): it for it in items}

            for fut in as_completed(futures):
                item = futures[fut]
                try:
                    res = fut.result()

                    # ------------------------------------------
                    # SAVE: we store normalized score (res["score"])
                    # raw_score is available but not saved to DB
                    # ------------------------------------------
                    if save and self.storage:
                        try:
                            details_json = json.dumps(res['details'], ensure_ascii=False)
                        except Exception:
                            details_json = json.dumps(str(res['details']), ensure_ascii=False)

                        # Save normalized score
                        self.storage.save_check(
                            uid=res['uid'],
                            text=res['text'],
                            score=res['score'],       # final normalized score
                            details=details_json
                        )

                    results.append(res)

                except Exception as e:
                    logger.exception("Error processing item %s: %s", item.get('uid'), e)

        return results
