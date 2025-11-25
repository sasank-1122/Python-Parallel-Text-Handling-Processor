"""Top-level text_processing package shim that delegates to `app.text_processing`.
This helps older scripts that import `text_processing.*` instead of `app.text_processing.*`.
"""
import os

# Prepend the app/text_processing directory to this package's __path__ so
# submodule imports like `text_processing.text_breaker` resolve to
# `app/text_processing/text_breaker.py`.
_app_text_processing = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'app', 'text_processing'))
if _app_text_processing not in __path__:
    __path__.insert(0, _app_text_processing)
