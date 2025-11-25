"""Top-level checker package shim that delegates to `app.checker`.
This allows imports like `from checker.rules import ...` to work.
"""
import os

_app_checker = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'app', 'checker'))
if _app_checker not in __path__:
    __path__.insert(0, _app_checker)
