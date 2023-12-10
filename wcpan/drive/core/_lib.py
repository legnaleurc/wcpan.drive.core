from collections.abc import Awaitable
from pathlib import Path, PurePath

from .exceptions import NodeNotFoundError
from .types import Node


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
    rv: list[str] = []
    for part in path.parts:
        if part == ".":
            continue
        elif part == ".." and rv[-1] != "/":
            rv.pop()
        else:
            rv.append(part)
    return PurePath(*rv)


def is_valid_name(name: str) -> bool:
    if name.find("\\") >= 0:
        return False
    path = Path(name)
    return path.name == name


async def else_none(aw: Awaitable[Node]) -> Node | None:
    try:
        return await aw
    except NodeNotFoundError:
        return None
