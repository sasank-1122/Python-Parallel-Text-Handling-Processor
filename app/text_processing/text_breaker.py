# app/text_processing/text_breaker.py
import re
from typing import List

def clean_text(text: str) -> str:
    """Collapse whitespace and trim."""
    if not isinstance(text, str):
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def break_text_into_groups(text: str, group_size: int = 500) -> List[str]:
    """
    Breaks text into groups of words (group_size words).
    Returns list of string chunks.
    """
    t = clean_text(text)
    if not t:
        return []
    words = t.split()
    groups = [words[i:i + group_size] for i in range(0, len(words), group_size)]
    return [" ".join(g) for g in groups]
