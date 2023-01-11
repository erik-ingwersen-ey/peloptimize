"""
I/O functions for reading and writing data to files.

Functions:

- ``read_json``: Read a JSON file.
- ``save_json``: Save object as a JSON file.

"""
from __future__ import annotations

from typing import Any
import json
from datetime import datetime
from pathlib import Path


__all__ = ["save_json", "read_json"]


def is_json_serializable(obj: object) -> bool:
    """Check if an object is JSON serializable.

    Parameters
    ----------
    obj : object
        The object to check.

    Returns
    -------
    bool
        ``True`` if the object is JSON serializable, ``False`` otherwise.
    """
    try:
        json.dumps(obj)
        return True
    except (TypeError, OverflowError):
        return False


def save_json(
    obj: object,
    file_path: str | Path | None = None,
    mode: str = "overwrite",
    encoding: str = "utf-8",
    indent: int = 4,
    **kwargs: Any,
):
    """Save an object to a JSON file.

    Parameters
    ----------
    obj : object
        The object to save. Object needs to be JSON serializable.
    file_path : str | Path | None, optional
        The filepath to save the object to. If None, the object will be saved
        to a file in the current working directory with a name based on the
        object's id and the current date and time.
    mode : str {'overwrite', 'append', 'raise'}, default 'overwrite'
        What to do in case the file already exists:

        - ``'overwrite'``: overwrite any pre-existing file.
        - ``'append'``: append to any pre-existing file.
        - ``'raise'``: raise a ``FileExistsError``.

    encoding : str, default 'utf-8'
        The encoding to use when saving the file.
    indent : int, default 4
        The number of spaces to indent the JSON file with.
    **kwargs : Any
        Additional keyword arguments to pass to ``json.dump``.
        Possible arguments include:

        skipkeys : bool default False
            If ``skipkeys=True``, then dict keys that are not of a
            basic type (``str``, ``int``, ``float``, ``bool``, ``None``)
            will be skipped instead of raising a ``TypeError``.
        ensure_ascii : bool default True
            If ``ensure_ascii=True``, the output is guaranteed to
            have all incoming non-ASCII characters escaped.
            If ``ensure_ascii=False``, these characters will be output as-is.
        check_circular : bool default True
            If ``check_circular=False``, then the circular reference
            check for container types will be skipped and a circular reference
            will result in an ``OverflowError`` (or worse).
        allow_nan : bool default True
            If ``allow_nan=False``, then it will be a ``ValueError`` to
            serialize out of range ``float`` values (``nan``, ``inf``, ``-inf``)
            in strict compliance of the JSON specification.
            If ``allow_nan=True``, their JavaScript equivalents
            (``NaN``, ``Infinity``, ``-Infinity``) will be used.
        separators : Tuple[str, str] | None default None
            If specified, separators should be an
            ``(item_separator, key_separator)`` tuple.
            The default is ``(', ', ': ')`` if indent is ``None``
            and ``(',', ': ')`` otherwise. To get the most compact
            JSON representation, you should specify ``(',', ':')``
            to eliminate whitespace.
        default=None
            If specified, :param:`default` should be a function that gets
            called for objects that can’t otherwise be serialized. It should
            return a JSON encodable version of the object or raise a
            ``TypeError``. If not specified, ``TypeError`` is raised.
        sort_keys : bool default False
            If ``sort_keys=True``, then the output of dictionaries
            is sorted by key.

    Raises
    ------
    ValueError
        If the object is not JSON serializable.
    FileExistsError
        If a file already exists at :param:`file_path`,
        and :param:`mode` is equal to ``'raise'``.

    Notes
    -----
    Depending on what kind of characters the object has, you should change the
    :param:`encoding` parameter accordingly. For example, if the object has
    characters from the Brazilian Portuguese alphabet, you might want to
    consider using ``encoding='iso-8859-1'``.
    """
    if not is_json_serializable(obj):
        raise ValueError(
            f'Object of type "{type(obj)}" is not JSON serializable.'
        )
    if file_path is None:
        file_path = (
            Path.cwd()
            .joinpath(
                f'{id(obj)}_{datetime.now().strftime("%Y-%m-%d %HH%MM%SS")}'
            )
            .with_suffix(".json")
        )
        print(f'No `file_path` provided, saving to: "{file_path}"')
    file_path = Path(file_path).with_suffix(".json")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.is_file() and mode == "append":
        mode = "w+"
    elif file_path.is_file() and mode == "raise":
        raise FileExistsError(
            f'File "{file_path}" already exists.'
            ' Use "mode=append" to append to the file, or'
            ' "mode=overwrite" to overwrite the file.'
        )
    else:
        mode = "w"
    with open(file_path, mode=mode, encoding=encoding) as fp:
        json.dump(obj, fp, indent=indent, **kwargs)


def read_json(filepath: str | Path, encoding: str = 'utf-8') -> dict:
    """Load a JSON file.

    Parameters
    ----------
    filepath : str | Path
        The filepath to load the JSON file from.
    encoding : str, default 'utf-8'
        The encoding to use when saving the file.

    Returns
    -------
    dict
        The loaded JSON file contents as a Python dictionary.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file is not a valid JSON file, and cannot be loaded.
    """
    filepath = Path(filepath).with_suffix(".json")
    if not filepath.is_file():
        raise FileNotFoundError(f'File "{filepath}" does not exist.')
    with open(filepath, mode="r", encoding=encoding) as fp:
        try:
            return json.load(fp)
        except json.JSONDecodeError as err:
            raise ValueError(f'File "{filepath}" is not a JSON file.') from err
