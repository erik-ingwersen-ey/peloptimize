from __future__ import annotations

import os
import re
import shutil
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path

_canonicalize_regex = re.compile("[-_]+")


def canonicalize_name(name: str) -> str:
    return _canonicalize_regex.sub("-", name).lower()


def module_name(name: str) -> str:
    return canonicalize_name(name).replace(".", "_").replace("-", "_")


def is_dir_writable(path: Path | str, create: bool = False) -> bool:
    try:
        path = Path(path)
        if not path.exists():
            if not create:
                return False
            path.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=str(path)):
            pass
    except (IOError, OSError):
        return False
    return True


def safe_rmtree(path):
    if Path(path).is_symlink():
        return os.unlink(str(path))


def _del_ro(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


@contextmanager
def temporary_directory(*args, **kwargs):
    name = tempfile.mkdtemp(*args, **kwargs)
    yield name
    shutil.rmtree(name, onerror=_del_ro)
