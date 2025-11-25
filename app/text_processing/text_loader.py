# app/text_processing/text_loader.py
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List
from .text_breaker import clean_text

def _read_file(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
        return fh.read()

def load_file(file_path: str) -> str:
    """Public wrapper with file existence check."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    return _read_file(file_path)

def load_all_texts(folder_path: str, ext: str = ".txt", max_workers: int = 4) -> List[str]:
    """Load all texts from folder using threads. Returns list of strings."""
    if not os.path.isdir(folder_path):
        raise NotADirectoryError(folder_path)
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith(ext)]
    texts = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for content in ex.map(_read_file, files):
            texts.append(clean_text(content))
    return texts
