__all__ = (
    "ConfigurationDict",
    "get_default_configuration",
    "get_default_config_path",
    "get_default_data_path",
    "create_executor",
    "resolve_path",
    "normalize_path",
    "get_mime_type",
    "is_valid_name",
    "get_utc_now",
)

from concurrent.futures import Executor, ProcessPoolExecutor
from pathlib import Path, PurePath
from typing import TypedDict
import datetime
import importlib
import mimetypes
import multiprocessing
import signal

from .types import PathOrString


class ConfigurationDict(TypedDict):
    version: int
    driver: str
    database: str
    middleware: list[str]


def get_default_configuration() -> ConfigurationDict:
    return {
        "version": 1,
        "driver": "",
        "database": "",
        "middleware": [],
    }


def get_default_config_path() -> Path:
    path = Path("~/.config")
    path = path.expanduser()
    path = path / "wcpan.drive"
    return path


def get_default_data_path() -> Path:
    path = Path("~/.local/share")
    path = path.expanduser()
    path = path / "wcpan.drive"
    return path


def create_executor() -> Executor:
    if multiprocessing.get_start_method() == "spawn":
        return ProcessPoolExecutor(initializer=initialize_worker)
    else:
        return ProcessPoolExecutor()


def initialize_worker() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def resolve_path(
    from_: PurePath,
    to: PurePath,
) -> PurePath:
    rv = from_
    for part in to.parts:
        if part == ".":
            continue
        elif part == "..":
            rv = rv.parent
        else:
            rv = rv / part
    return rv


def normalize_path(path: PurePath) -> PurePath:
    if not path.is_absolute():
        raise ValueError("only accepts absolute path")
    rv = []
    for part in path.parts:
        if part == ".":
            continue
        elif part == ".." and rv[-1] != "/":
            rv.pop()
        else:
            rv.append(part)
    return PurePath(*rv)


def import_class(class_path: str) -> type:
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    class_ = getattr(module, class_name)
    return class_


def get_mime_type(path: PathOrString) -> str:
    type_, dummy_encoding = mimetypes.guess_type(path)
    if not type_:
        return "application/octet-stream"
    return type_


def is_valid_name(name: str) -> bool:
    if name.find("\\") >= 0:
        return False
    path = Path(name)
    return path.name == name


def get_utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)
