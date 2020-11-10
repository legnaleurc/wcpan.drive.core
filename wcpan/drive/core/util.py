from typing import List, TypedDict, BinaryIO, Optional, Type
import concurrent.futures
import importlib
import mimetypes
import multiprocessing
import os
import pathlib
import signal

from wcpan.logger import EXCEPTION

from .types import Node, PathOrString, MediaInfo
from .exceptions import (
    DownloadError,
    NodeConflictedError,
    UploadError,
)


class ConfigurationDict(TypedDict):

    version: int
    driver: str
    database: str
    middleware: List[str]


CHUNK_SIZE = 64 * 1024


def get_default_configuration() -> ConfigurationDict:
    return {
        'version': 1,
        'driver': None,
        'database': None,
    }


def get_default_config_path() -> pathlib.Path:
    path = pathlib.Path('~/.config')
    path = path.expanduser()
    path = path / 'wcpan.drive'
    return path


def get_default_data_path() -> pathlib.Path:
    path = pathlib.Path('~/.local/share')
    path = path.expanduser()
    path = path / 'wcpan.drive'
    return path


def create_executor() -> concurrent.futures.Executor:
    if multiprocessing.get_start_method() == 'spawn':
        return concurrent.futures.ProcessPoolExecutor(initializer=initialize_worker)
    else:
        return concurrent.futures.ProcessPoolExecutor()


def initialize_worker() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def resolve_path(
    from_: pathlib.PurePath,
    to: pathlib.PurePath,
) -> pathlib.PurePath:
    rv = from_
    for part in to.parts:
        if part == '.':
            continue
        elif part == '..':
            rv = rv.parent
        else:
            rv = rv / part
    return rv


def normalize_path(path: pathlib.PurePath) -> pathlib.PurePath:
    if not path.is_absolute():
        raise ValueError('only accepts absolute path')
    rv = []
    for part in path.parts:
        if part == '.':
            continue
        elif part == '..' and rv[-1] != '/':
            rv.pop()
        else:
            rv.append(part)
    return pathlib.PurePath(*rv)


async def download_to_local_by_id(
    drive: 'Drive',
    node_id: str,
    path: PathOrString,
) -> pathlib.Path:
    node = await drive.get_node_by_id(node_id)
    return await download_to_local(drive, node, path)


async def download_to_local(
    drive: 'Drive',
    node: Node,
    path: PathOrString,
) -> pathlib.Path:
    file_ = pathlib.Path(path)
    if not file_.is_dir():
        raise ValueError(f'{path} does not exist')

    # check if exists
    complete_path = file_.joinpath(node.name)
    if complete_path.is_file():
        return complete_path

    # exists but not a file
    if complete_path.exists():
        raise DownloadError(f'{complete_path} exists but is not a file')

    # if the file is empty, no need to download
    if node.size <= 0:
        open(complete_path, 'w').close()
        return complete_path

    # resume download
    tmp_path = complete_path.parent.joinpath(f'{complete_path.name}.__tmp__')
    if tmp_path.is_file():
        offset = tmp_path.stat().st_size
        if offset > node.size:
            raise DownloadError(
                f'local file size of `{complete_path}` is greater then remote'
                f' ({offset} > {node.size})')
    elif tmp_path.exists():
        raise DownloadError(f'{complete_path} exists but is not a file')
    else:
        offset = 0

    if offset < node.size:
        async with await drive.download(node) as fin:
            await fin.seek(offset)
            with open(tmp_path, 'ab') as fout:
                while True:
                    try:
                        async for chunk in fin:
                            fout.write(chunk)
                        break
                    except Exception as e:
                        EXCEPTION('wcpan.drive.core', e) << 'download'

                    offset = fout.tell()
                    await fin.seek(offset)

    # rename it back if completed
    os.rename(tmp_path, complete_path)

    return complete_path


async def upload_from_local_by_id(
    drive: 'Drive',
    parent_id: str,
    file_path: PathOrString,
    media_info: Optional[MediaInfo],
    *,
    exist_ok: bool = False,
) -> Node:
    node = await drive.get_node_by_id(parent_id)
    return await upload_from_local(
        drive,
        node,
        file_path,
        media_info,
        exist_ok=exist_ok,
    )


async def upload_from_local(
    drive: 'Drive',
    parent_node: Node,
    file_path: PathOrString,
    media_info: Optional[MediaInfo],
    *,
    exist_ok: bool = False,
) -> Node:
    # sanity check
    file_ = pathlib.Path(file_path).resolve()
    if not file_.is_file():
        raise UploadError('invalid file path')

    file_name = file_.name
    total_file_size = file_.stat().st_size
    mime_type = get_mime_type(file_path)

    try:
        fout = await drive.upload(
            parent_node=parent_node,
            file_name=file_name,
            file_size=total_file_size,
            mime_type=mime_type,
            media_info=media_info,
        )
    except NodeConflictedError as e:
        if not exist_ok:
            raise
        return e.node

    async with fout:
        with open(file_path, 'rb') as fin:
            while True:
                try:
                    await upload_feed(fin, fout)
                    break
                except UploadError as e:
                    raise
                except Exception as e:
                    EXCEPTION('wcpan.drive.core', e) << 'upload feed'

                await upload_continue(fin, fout)

    node = await fout.node()
    return node


async def upload_feed(fin: BinaryIO, fout: BinaryIO) -> None:
    while True:
        chunk = fin.read(CHUNK_SIZE)
        if not chunk:
            break
        await fout.write(chunk)


async def upload_continue(fin: BinaryIO, fout: BinaryIO) -> None:
    offset = await fout.tell()
    await fout.seek(offset)
    fin.seek(offset, os.SEEK_SET)


async def find_duplicate_nodes(
    drive: 'Drive',
    root_node: Node = None,
) -> List[List[Node]]:
    if not root_node:
        root_node = await drive.get_root_node()

    rv = []
    async for dummy_root, folders, files in drive.walk(root_node):
        nodes = folders + files
        seen = {}
        for node in nodes:
            if node.name not in seen:
                seen[node.name] = [node]
            else:
                seen[node.name].append(node)
        for nodes in seen.values():
            if len(nodes) > 1:
                rv.append(nodes)

    return rv


def import_class(class_path: str) -> Type:
    module_path, class_name = class_path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    class_ = getattr(module, class_name)
    return class_


def get_mime_type(path: PathOrString):
    type_, dummy_encoding = mimetypes.guess_type(path)
    if not type_:
        return 'application/octet-stream'
    return type_
