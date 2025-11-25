# app/utils.py
import json
import os
from dotenv import load_dotenv
import logging
from typing import Any, Optional, Union

load_dotenv()

logger = logging.getLogger(__name__)

import hashlib

def compute_text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def get_logger(name: str = __name__, level: int = logging.INFO, logfile: Optional[str] = None) -> logging.Logger:
    """Return a configured logger. Add rotating file handler if logfile provided."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if logfile:
        try:
            from logging.handlers import RotatingFileHandler
            fh = RotatingFileHandler(logfile, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            logger.exception("Failed to create rotating file handler")
    return logger

def load_json(path: str) -> Any:
    if not os.path.exists(path):
        raise FileNotFoundError(f"JSON file not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as e:
        logger.error("Error decoding JSON file '%s': %s", path, e)
        raise

def save_json(data: Any, path: str, indent: int = 2) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=indent)
        logger.debug("Saved JSON to %s", path)
    except Exception:
        logger.exception("Failed to save JSON to %s", path)
        raise

def get_env(key: str, default: Optional[Union[str, bool, int, float]] = None, cast_type: type = str):
    v = os.environ.get(key)
    if v is None:
        return default
    if cast_type == bool:
        return str(v).lower() in ("1", "true", "yes", "on")
    try:
        return cast_type(v)
    except Exception:
        logger.warning("Failed to cast env var %s to %s; returning raw string", key, getattr(cast_type, "__name__", str))
        return v

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    logger.debug("Ensured dir %s exists", path)
