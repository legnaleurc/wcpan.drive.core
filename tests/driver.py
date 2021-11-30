from typing import List, Tuple, AsyncGenerator, Optional

from wcpan.drive.core.abc import (
    Hasher,
    ReadableFile,
    RemoteDriver,
    Middleware,
    WritableFile,
)
from wcpan.drive.core.types import (
    Node,
    ChangeDict,
    PrivateDict,
    MediaInfo,
    ReadOnlyContext,
)


class FakeMiddleware(Middleware):

    @classmethod
    def get_version_range(cls):
        return (1, 1)

    def __init__(self, context: ReadOnlyContext, driver: RemoteDriver) -> None:
        self._context = context
        self._driver = driver

    async def __aenter__(self) -> Middleware:
        return self

    async def __aexit__(self, et, ev, tb) -> bool:
        pass

    async def get_initial_check_point(self) -> str:
        return await self._driver.get_initial_check_point()

    async def fetch_root_node(self) -> Node:
        return await self._driver.fetch_root_node()

    async def fetch_changes(self,
        check_point: str,
    ) -> AsyncGenerator[Tuple[str, List[ChangeDict]], None]:
        async for check_point, changes in self._driver.fetch_changes(check_point):
            yield check_point, changes

    async def create_folder(self,
        parent_node: Node,
        folder_name: str,
        private: Optional[PrivateDict],
        exist_ok: bool,
    ) -> Node:
        return await self._driver.create_folder(
            parent_node=parent_node,
            folder_name=folder_name,
            private=private,
            exist_ok=exist_ok,
        )

    async def rename_node(self,
        node: Node,
        new_parent: Optional[Node],
        new_name: Optional[str],
    ) -> Node:
        return await self._driver.rename_node(
            node=node,
            new_parent=new_parent,
            new_name=new_name,
        )

    async def trash_node(self, node: Node) -> None:
        return await self._driver.trash_node(node=node)

    async def download(self, node: Node) -> ReadableFile:
        return await self._driver.download(node=node)

    async def upload(self,
        parent_node: Node,
        file_name: str,
        file_size: Optional[int],
        mime_type: Optional[str],
        media_info: Optional[MediaInfo],
        private: Optional[PrivateDict],
    ) -> WritableFile:
        return await self._driver.upload(
            parent_node=parent_node,
            file_name=file_name,
            file_size=file_size,
            mime_type=mime_type,
            media_info=media_info,
            private=private,
        )

    async def get_hasher(self) -> Hasher:
        return await self._driver.get_hasher()
