"""Top-level storage package shim that delegates to `app.storage`.
This allows imports like `from storage.storage import Storage` to work.
"""
import os

_app_storage = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'app', 'storage'))
if _app_storage not in __path__:
    __path__.insert(0, _app_storage)
