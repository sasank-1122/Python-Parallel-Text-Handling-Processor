"""
storage_improver.py

Analyzes stored text + scoring results, finds:
  - common words
  - common phrases
  - rule hit frequencies
  - suggestions for new keyword rules

This version includes FIXES for:
  - details stored as JSON string
  - safely decoding rule hits
  - skipping invalid/corrupt details
"""

import os
import json
import logging
from collections import Counter, defaultdict
from typing import List, Dict, Any

from app.utils import ensure_dir, save_json

logger = logging.getLogger(__name__)


class StorageImprover:
    def __init__(self, storage):
        self.storage = storage

    # -------------------------------------------------------------
    # SAFE DETAILS PARSER (Important Fix)
    # -------------------------------------------------------------
    def _parse_details(self, raw_details):
        """
        Convert DB-stored JSON string → list of dicts.
        Avoids crashes like: AttributeError: 'str' object has no attribute 'get'
        """
        if raw_details is None:
            return []

        # Already parsed list?
        if isinstance(raw_details, list):
            return raw_details

        # Convert JSON string → Python list
        if isinstance(raw_details, str):
            try:
                parsed = json.loads(raw_details)
                if isinstance(parsed, list):
                    return parsed
                return []
            except Exception:
                logger.warning("Failed to JSON-decode details: %s", raw_details)
                return []

        # Unknown type
        return []

    # -------------------------------------------------------------
    # HELPER: tokenize text
    # -------------------------------------------------------------
    def _tokenize(self, text: str) -> List[str]:
        return [w.lower() for w in text.split() if w.strip()]

    # -------------------------------------------------------------
    # HELPER: generate bigrams & trigrams
    # -------------------------------------------------------------
    def _generate_phrases(self, words):
        phrases = []
        for i in range(len(words) - 1):
            phrases.append(words[i] + " " + words[i + 1])
        for i in range(len(words) - 2):
            phrases.append(words[i] + " " + words[i + 2])
        return phrases

    # -------------------------------------------------------------
    # 1. WORD/PHRASE FREQUENCY ANALYSIS
    # -------------------------------------------------------------
    def analyze_word_frequency(self, limit=500):
        rows = self.storage.query_checks(limit=limit)
        logger.info("Analyzing %d texts for word frequency...", len(rows))

        word_counter = Counter()
        phrase_counter = Counter()

        for row in rows:
            text = row.get("text", "")
            words = self._tokenize(text)
            word_counter.update(words)

            phrases = self._generate_phrases(words)
            phrase_counter.update(phrases)

        logger.info("Found %d unique words", len(word_counter))
        logger.info("Found %d unique phrases", len(phrase_counter))

        return word_counter, phrase_counter

    # -------------------------------------------------------------
    # 2. RULE HIT ANALYSIS (Fixed)
    # -------------------------------------------------------------
    def analyze_rule_hits(self, limit=500):
        rows = self.storage.query_checks(limit=limit)
        logger.info("Analyzing rule hits for %d rows...", len(rows))

        hit_counter = Counter()

        for row in rows:
            raw_details = row.get("details")

            details = self._parse_details(raw_details)
            if not isinstance(details, list):
                continue

            for d in details:
                if not isinstance(d, dict):
                    continue
                rule_id = d.get("rule_id")
                if rule_id is not None:
                    hit_counter[rule_id] += 1

        logger.info("Computed rule hit frequency for %d rules", len(hit_counter))
        return hit_counter

    # -------------------------------------------------------------
    # 3. SUGGEST RULES (keyword_any)
    # -------------------------------------------------------------
    def generate_rule_suggestions(self, word_counter, min_freq=5):
        suggestions = []

        for word, freq in word_counter.items():
            if freq >= min_freq and len(word) > 3:
                suggestions.append({
                    "type": "keyword_any",
                    "keywords": [word],
                    "score": +1,
                    "source": "auto-generated"
                })

        return suggestions

    # -------------------------------------------------------------
    # 4. MAIN ENTRY
    # -------------------------------------------------------------
    def run(self, limit=500, min_freq=5, auto_update=False):
        logger.info("=== Running Storage Improver ===")

        # 1. Word analysis
        word_counter, phrase_counter = self.analyze_word_frequency(limit)

        # 2. Rule hits
        rule_hits = self.analyze_rule_hits(limit)

        # 3. Suggestions
        suggestions = self.generate_rule_suggestions(word_counter, min_freq=min_freq)

        # Store outputs
        ensure_dir("improver_output")

        save_json(
            {
                "words": word_counter.most_common(200),
                "phrases": phrase_counter.most_common(200),
                "rule_hits": rule_hits.most_common(200),
                "suggestions": suggestions,
            },
            "improver_output/report.json"
        )

        logger.info("Generated %d rule suggestions.", len(suggestions))

        # 4. Auto-update rules.json if required
        if auto_update:
            rules_path = "data/rules.json"
            try:
                with open(rules_path, "r") as f:
                    rules = json.load(f)
            except FileNotFoundError:
                rules = []

            # Extend rule set
            rules.extend(suggestions)

            # Save back
            with open(rules_path, "w") as f:
                json.dump(rules, f, indent=4)

            logger.info("rules.json updated with %d new rules.", len(suggestions))

        return suggestions
