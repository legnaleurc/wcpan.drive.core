from typing import AsyncGenerator, Optional

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
        return (3, 3)

    def __init__(self, context: ReadOnlyContext, driver: RemoteDriver) -> None:
        self._context = context
        self._driver = driver

    async def __aenter__(self) -> Middleware:
        return self

    async def __aexit__(self, et, ev, tb) -> bool:
        pass

    @property
    def remote(self):
        return self._driver

    async def get_initial_check_point(self) -> str:
        return await self._driver.get_initial_check_point()

    async def fetch_root_node(self) -> Node:
        return await self._driver.fetch_root_node()

    async def fetch_changes(
        self,
        check_point: str,
    ) -> AsyncGenerator[tuple[str, list[ChangeDict]], None]:
        async for check_point, changes in self._driver.fetch_changes(check_point):
            yield check_point, changes

    async def create_folder(
        self,
        parent_node: Node,
        folder_name: str,
        *,
        exist_ok: bool,
        private: PrivateDict | None,
    ) -> Node:
        return await self._driver.create_folder(
            parent_node=parent_node,
            folder_name=folder_name,
            private=private,
            exist_ok=exist_ok,
        )

    async def rename_node(
        self,
        node: Node,
        *,
        new_parent: Node | None,
        new_name: str | None,
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

    async def is_authorized(self) -> bool:
        return True

    async def get_oauth_url(self) -> str:
        return ""

    async def set_oauth_token(self, token: str) -> None:
        pass
