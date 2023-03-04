import contextlib
import hashlib
import os
import pathlib
from tempfile import TemporaryDirectory
from typing import AsyncIterator
from unittest.mock import AsyncMock

from .abc import RemoteDriver, ReadableFile, WritableFile, Hasher
from .drive import DriveFactory, ReadOnlyContext
from .types import Node, ChangeDict, PrivateDict, MediaInfo, NodeDict
from .util import get_utc_now


@contextlib.contextmanager
def test_factory(driver_class: str = None, middleware_list: list[str] = None):
    with TemporaryDirectory() as work_folder:
        work_path = pathlib.Path(work_folder)
        config_path = work_path / "config"
        config_path.mkdir()
        data_path = work_path / "data"
        data_path.mkdir()

        factory = DriveFactory()
        factory.config_path = config_path
        factory.data_path = data_path
        factory.database = data_path / "nodes.sqlite"
        factory.driver = (
            "wcpan.drive.core.test.TestDriver" if driver_class is None else driver_class
        )
        factory.middleware_list = [] if middleware_list is None else middleware_list

        yield factory


class TestDriver(RemoteDriver):
    @classmethod
    def get_version_range(cls):
        return (3, 3)

    def __init__(self, context: ReadOnlyContext) -> None:
        self.mock = MockManager()
        self.pseudo = PseudoManager()
        self._context = context
        self._root: pathlib.Path = None
        self._raii = None

    async def __aenter__(self) -> RemoteDriver:
        with contextlib.ExitStack() as stack:
            root = stack.enter_context(TemporaryDirectory())
            self._root = pathlib.Path(root)
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, et, ev, tb) -> bool:
        self._root = None
        self._raii.close()
        self._raii = None

    @property
    def remote(self):
        return None

    async def get_initial_check_point(self) -> str:
        return str(self.pseudo.INITIAL_CHECK_POINT)

    async def fetch_root_node(self) -> Node:
        node = self.pseudo.build_node().node
        return node

    async def fetch_changes(self, check_point: str):
        yield str(self.pseudo.check_point), self.pseudo.changes
        self.pseudo.consume()

    async def create_folder(
        self,
        parent_node: Node,
        folder_name: str,
        *,
        exist_ok: bool,
        private: PrivateDict | None,
    ) -> Node:
        await self.mock.create_folder(parent_node, folder_name, private, exist_ok)

        builder = self.pseudo.build_node()
        builder.to_folder(folder_name, parent_node)
        folder = builder.commit()

        return folder

    async def rename_node(
        self,
        node: Node,
        *,
        new_parent: Node | None,
        new_name: str | None,
    ) -> Node:
        await self.mock.rename_node(node, new_parent, new_name)

        dict_ = node.to_dict()
        if new_parent:
            dict_["parent_list"] = [new_parent.id_]
        if new_name:
            dict_["name"] = new_name

        self.pseudo.update(dict_)

        return Node.from_dict(dict_)

    async def trash_node(self, node: Node) -> None:
        await self.mock.trash_node(node)

        dict_ = node.to_dict()
        dict_["trashed"] = True

        self.pseudo.update(dict_)

    async def download(self, node: Node) -> ReadableFile:
        await self.mock.download(node)
        return NodeReader(self._root, node)

    async def upload(
        self,
        parent_node: Node,
        file_name: str,
        *,
        file_size: int | None,
        mime_type: str | None,
        media_info: MediaInfo | None,
        private: PrivateDict | None,
    ) -> WritableFile:
        await self.mock.upload(
            parent_node,
            file_name,
            file_size,
            mime_type,
            media_info,
            private,
        )
        return NodeWriter(
            root=self._root,
            pseudo=self.pseudo,
            parent_node=parent_node,
            file_name=file_name,
            mime_type=mime_type,
            media_info=media_info,
        )

    async def get_hasher(self) -> Hasher:
        await self.mock.get_hasher()
        return NodeHasher()

    async def is_authorized(self) -> bool:
        return True

    async def get_oauth_url(self) -> str:
        return ""

    async def set_oauth_token(self, token: str) -> None:
        pass


class MockManager(object):
    def __init__(self):
        self.create_folder = AsyncMock()
        self.rename_node = AsyncMock()
        self.trash_node = AsyncMock()
        self.download = AsyncMock()
        self.upload = AsyncMock()
        self.get_hasher = AsyncMock()

    def reset_all_mocks(self, *, return_value: bool = False, side_effect: bool = False):
        self.create_folder.reset_mock(
            return_value=return_value, side_effect=side_effect
        )
        self.rename_node.reset_mock(return_value=return_value, side_effect=side_effect)
        self.trash_node.reset_mock(return_value=return_value, side_effect=side_effect)
        self.download.reset_mock(return_value=return_value, side_effect=side_effect)
        self.upload.reset_mock(return_value=return_value, side_effect=side_effect)
        self.get_hasher.reset_mock(return_value=return_value, side_effect=side_effect)


class PseudoManager(object):
    INITIAL_CHECK_POINT = 1

    def __init__(self):
        self.check_point = 1
        self.changes: list[ChangeDict] = []
        self._id = 0

    def next_id(self) -> str:
        id_ = self._id
        self._id += 1
        return f"__ID_{id_}__"

    def build_node(self) -> "NodeBuilder":
        return NodeBuilder(self)

    def update(self, dict_: NodeDict) -> None:
        self.changes.append(
            {
                "removed": False,
                "node": dict_,
            }
        )
        self.check_point += 1

    def delete(self, id_: str) -> None:
        self.changes.append(
            {
                "removed": True,
                "id": id_,
            }
        )
        self.check_point += 1

    def consume(self) -> None:
        self.changes = []


class NodeBuilder(object):
    def __init__(self, pseudo: PseudoManager):
        self.pseudo = pseudo
        self.dict: NodeDict = {
            "id": "__ID_ROOT__",
            "name": None,
            "is_folder": True,
            "trashed": False,
            "created": get_utc_now().isoformat(),
            "modified": get_utc_now().isoformat(),
            "parent_list": [],
            "size": None,
            "mime_type": None,
            "hash": None,
            "image": None,
            "video": None,
            "private": None,
        }

    @property
    def node(self):
        return Node.from_dict(self.dict)

    def to_trashed(self, trashed: bool = True):
        self.dict["trashed"] = trashed
        return self

    def to_folder(self, name: str, parent: Node):
        self.dict["id"] = self.pseudo.next_id()
        self.dict["name"] = name
        self.dict["parent_list"] = [parent.id_]
        return self

    def to_file(self, size: int, hash_: str, mime_type: str):
        self.dict["is_folder"] = False
        self.dict["size"] = size
        self.dict["hash"] = hash_
        self.dict["mime_type"] = mime_type
        return self

    def to_image(self, width: int, height: int):
        self.dict["image"] = {
            "width": width,
            "height": height,
        }
        return self

    def to_video(self, width: int, height: int, ms_duration: int):
        self.dict["video"] = {
            "width": width,
            "height": height,
            "ms_duration": ms_duration,
        }
        return self

    def commit(self):
        self.pseudo.update(self.dict)
        return self.node


class NodeHasher(Hasher):
    def __init__(self):
        self._hasher = hashlib.md5()

    def __getstate__(self):
        return hashlib.md5

    def __setstate__(self, class_):
        self._hasher = class_()

    def update(self, data: bytes) -> None:
        self._hasher.update(data)

    def digest(self) -> bytes:
        return self._hasher.digest()

    def hexdigest(self) -> str:
        return self._hasher.hexdigest()

    def copy(self) -> Hasher:
        return NodeHasher()


class NodeReader(ReadableFile):
    def __init__(self, root: pathlib.Path, node: Node):
        self._root = root
        self._node = node
        self._fin = None
        self._raii = None

    async def __aenter__(self) -> ReadableFile:
        with contextlib.ExitStack() as stack:
            path = self._root / self._node.id_
            self._fin = stack.enter_context(path.open("rb"))
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        self._fin = None
        self._raii.close()
        self._raii = None

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = self._fin.read(8192)
            if not chunk:
                return
            yield chunk

    async def read(self, length: int) -> bytes:
        return self._fin.read(length)

    async def seek(self, offset: int) -> None:
        self._fin.flush()
        self._fin.seek(offset, os.SEEK_SET)

    async def node(self) -> Node:
        return self._node


class NodeWriter(WritableFile):
    def __init__(
        self,
        root: pathlib.Path,
        pseudo: PseudoManager,
        parent_node: Node,
        file_name: str,
        mime_type: str | None,
        media_info: MediaInfo | None,
    ):
        self._root = root
        self._pseudo = pseudo
        self._parent_node = parent_node
        self._file_name = file_name
        self._mime_type = mime_type
        self._media_info = media_info
        self._node = None
        self._fout = None
        self._raii = None

    async def __aenter__(self) -> WritableFile:
        self._node = None
        with contextlib.ExitStack() as stack:
            path = self._root / "_"
            self._fout = stack.enter_context(path.open("wb"))
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        self._fout.flush()
        self._fout = None
        self._raii.close()
        self._raii = None

        path = self._root / "_"
        stat = path.stat()
        hasher = NodeHasher()
        with path.open("rb") as fin:
            while True:
                chunk = fin.read(8092)
                if not chunk:
                    break
                hasher.update(chunk)
        hash_ = hasher.hexdigest()

        builder = self._pseudo.build_node()
        builder.to_folder(self._file_name, self._parent_node)
        builder.to_file(stat.st_size, hash_, self._mime_type)
        if self._media_info:
            if self._media_info.is_image:
                builder.to_image(self._media_info.width, self._media_info.height)
            elif self._media_info.is_video:
                builder.to_video(
                    self._media_info.width,
                    self._media_info.height,
                    self._media_info.ms_duration,
                )
        path.rename(self._root / builder.node.id_)
        self._node = builder.commit()

    async def tell(self) -> int:
        return self._fout.tell()

    async def seek(self, offset: int) -> None:
        self._fout.flush()
        self._fout.seek(offset, os.SEEK_SET)

    async def write(self, chunk: bytes) -> int:
        return self._fout.write(chunk)

    async def node(self) -> Node | None:
        return self._node
