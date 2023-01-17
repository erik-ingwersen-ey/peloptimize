"""
Helper functions for working with files.
"""
from __future__ import annotations

import os
import re
import shutil
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, List


_canonicalize_regex = re.compile("[-_]+")


def canonicalize_name(name: str) -> str:
    """Convert a name to a canonical form."""
    return _canonicalize_regex.sub("-", name).lower()


def module_name(name: str) -> str:
    """Convert a name to a valid module name."""
    return canonicalize_name(name).replace(".", "_").replace("-", "_")


def is_dir_writable(path: Path | str, create: bool = False) -> bool:
    """Check if a directory is writable.

    Parameters
    ----------
    path : Path | str
        The path to the directory to check.
    create : bool, default=False
        Whether to create the directory.

    Returns
    -------
    bool
        Whether the directory is writable.
    """
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


def safe_rmtree(path: Path | str) -> None:
    """Remove a directory tree, even if it is read-only.

    Parameters
    ----------
    path : Path | str
        The path to the directory to remove.
    """
    if Path(path).is_symlink():
        os.unlink(str(path))


def _del_ro(action, name, exc):  # pylint: disable=unused-argument
    """Helper function to remove read-only files."""
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


@contextmanager
def temporary_directory(*args, **kwargs) -> Generator[Any, Any, None]:
    """Create a temporary directory.

    Parameters
    ----------
    args : Any
        Positional arguments to pass to :func:`tempfile.mkdtemp`.
    kwargs : Any
        Keyword arguments to pass to :func:`tempfile.mkdtemp`.

    Yields
    ------
    Generator[Any, Any, None]
        The path to the temporary directory.
    """
    name = tempfile.mkdtemp(*args, **kwargs)
    yield name
    shutil.rmtree(name, onerror=_del_ro)


def find_file(
    base_dir: str | Path | None = None,
    filename: str | Path | None = None,
    suffix: str | None = None,
    recursive: bool = True,
    max_upper_dirs: int | None = 2,
) -> List[str]:
    """
    Find filepaths matching a given name or suffix in a directory.

    Function tries to find filepaths that match :param:`filename`, or that
    contain :param:`filename` in its name. When no :param:`filename` is
    specified, function will find filepaths that contain the suffix
    specified by :param:`suffix`.

    Parameters
    ----------
    base_dir : str | Path | None
        The directory to search in. Default is the current working directory.
    filename : str | Path | None
        The name of the file to search for. This parameter is obligatory, when
        :param:`suffix` is equal to None.
    suffix : str | None
        The file suffix to search for. This parameter is obligatory, when
        :param:`filename` is equal to None.
    recursive : bool, default=True
            Search recursively through the directory and its subdirectories.
    max_upper_dirs : int | None, default=2
        The maximum number of parent directories to search in. This parameter
        only gets used when :param:`recursive` is set to True.

    Returns
    -------
    List[str]
        List of file paths that match the search criteria.

    Raises
    ------
    ValueError
        When neither :param:`filename` nor :param:`suffix` is specified.
    FileNotFoundError
        When no filepath matches the specified criteria.

    Examples
    --------
    >>> find_file(filename='sr', recursive=False)
    ['/Users/erikingwersen/Documents/GitHub/peloptimize/src']
    >>> find_file(filename='datasets', suffix='.joblib')
    ['/Users/erikingwersen/Documents/GitHub/peloptimize/datasets.joblib',
     '/Users/erikingwersen/Documents/GitHub/peloptimize/tmp/us8/datasets
     .joblib']
    >>> find_file(suffix='.joblib')
    ['/Users/erikingwersen/Documents/GitHub/peloptimize/datasets.joblib',
     '/Users/erikingwersen/Documents/GitHub/peloptimize/tmp/us8/df_sql_raw
     .joblib',
     '/Users/erikingwersen/Documents/GitHub/peloptimize/tmp/us8/datasets
     .joblib',
     '/Users/erikingwersen/Documents/GitHub/peloptimize/tmp/us8/df_sql.joblib']
    >>> find_file()  # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ValueError: Either `filename` or `suffix` must be specified.
    """
    base_dir = Path.cwd() if base_dir is None else Path(base_dir)
    if isinstance(suffix, str):
        suffix = suffix if suffix.startswith(".") else f".{suffix}"
    if filename is None and suffix is None:
        raise ValueError(
            "Either `filename`, `suffix`, or both must be specified."
        )
    if filename is None:
        filename = Path(suffix)
    else:
        filename = Path(filename)
        if isinstance(suffix, str):
            filename = filename.with_suffix(suffix)
    glob_pattern = ""
    current_dir = ""
    if recursive:
        glob_pattern = "**/"
    else:
        max_upper_dirs = 1
    while max_upper_dirs > 0:
        glob_patterns = [
            f"{current_dir}{glob_pattern}{filename}",
            f"{current_dir}{glob_pattern}*{filename}",
            f"{current_dir}{glob_pattern}*{filename}*",
            f"{current_dir}{glob_pattern}{filename}*",
        ]
        for glob in glob_patterns:
            matches = list(map(str, base_dir.glob(glob)))
            if len(matches) > 0:
                return matches
        current_dir = f"../{current_dir}"
        max_upper_dirs -= 1
    raise FileNotFoundError(
        f"Could not find {filename} in {base_dir} or any of its "
        f"parent directories."
    )
