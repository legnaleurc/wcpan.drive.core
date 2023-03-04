from __future__ import annotations


__all__ = (
    "ReadableFile",
    "Hasher",
    "WritableFile",
    "Middleware",
    "RemoteDriver",
)


from abc import ABCMeta, abstractmethod
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Optional,
    Type,
)
from types import TracebackType

from .types import (
    ChangeDict,
    MediaInfo,
    Node,
    PrivateDict,
)


class ReadableFile(metaclass=ABCMeta):
    """
    An async readable file interface.

    Should support async iterator and async context manager.

    Can be used like this:
    ```
    async with ReadableFile(...) as fin:
        async for chunk in fin:
            ...
    ```
    """

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[bytes]:
        ...

    @abstractmethod
    async def __aenter__(self) -> ReadableFile:
        ...

    @abstractmethod
    async def __aexit__(
        self,
        et: Optional[Type[BaseException]],
        ev: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        ...

    @abstractmethod
    async def read(self, length: int) -> bytes:
        """
        Read at most `length` bytes.
        """

    @abstractmethod
    async def seek(self, offset: int) -> None:
        """
        Seek to `offset` position. Always starts from the begining.
        """

    @abstractmethod
    async def node(self) -> Node:
        """
        Get the node being read.
        """


class Hasher(metaclass=ABCMeta):
    """
    Hash calculator.

    MUST be pickleable to work with multi-processes.
    """

    @abstractmethod
    def update(self, data: bytes) -> None:
        """
        Put `data` into the stream.
        """

    @abstractmethod
    def digest(self) -> bytes:
        """
        Get raw digest.
        """

    @abstractmethod
    def hexdigest(self) -> str:
        """
        Get hex digest.
        """

    @abstractmethod
    def copy(self) -> Hasher:
        """
        Return a copy to self. Does not require clone the state.
        """


class WritableFile(metaclass=ABCMeta):
    """
    An async writable file interface.

    Should support and async context manager.

    Can be used like this:
    ```
    async with WritableFile(...) as fout:
        ...
    ```
    """

    @abstractmethod
    async def __aenter__(self) -> WritableFile:
        ...

    @abstractmethod
    async def __aexit__(
        self,
        et: Optional[Type[BaseException]],
        ev: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        ...

    @abstractmethod
    async def tell(self) -> int:
        """
        Get current position.
        """

    @abstractmethod
    async def seek(self, offset: int) -> None:
        """
        Seek to `offset` position. Always starts from the begining.
        """

    @abstractmethod
    async def write(self, chunk: bytes) -> int:
        """
        Writes `chunk` to the stream.
        Returns actual written byte length.
        """

    @abstractmethod
    async def node(self) -> Optional[Node]:
        """
        Get the wrote node. May be `None` if write failed.
        """


class RemoteDriver(metaclass=ABCMeta):
    """
    Provides actions to cloud drives.

    Must be used with async context manager.
    """

    @classmethod
    @abstractmethod
    def get_version_range(cls) -> tuple[int, int]:
        """
        Get competible API version range for this driver.

        The tuple is (minimal, maximum), inclusive.
        """

    @property
    @abstractmethod
    def remote(self) -> Optional[RemoteDriver]:
        """
        Get the decorated remote driver, if any.
        """

    @abstractmethod
    async def __aenter__(self) -> RemoteDriver:
        ...

    @abstractmethod
    async def __aexit__(
        self,
        et: Optional[Type[BaseException]],
        ev: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        ...

    @abstractmethod
    async def get_initial_check_point(self) -> str:
        """
        Get the initial check point.
        """

    @abstractmethod
    async def fetch_root_node(self) -> Node:
        """
        Fetch the root node.
        """

    @abstractmethod
    async def fetch_changes(
        self,
        check_point: str,
    ) -> AsyncGenerator[tuple[str, list[ChangeDict]], None]:
        """
        Fetch changes starts from `check_point`.

        Will be used like this:
        ```
        async for next_check_point, changes in self.fetch_changes('...'):
            ...
        ```
        So you should yield a page for every iteration.
        """

    @abstractmethod
    async def create_folder(
        self,
        parent_node: Node,
        folder_name: str,
        *,
        exist_ok: bool,
        private: Optional[PrivateDict],
    ) -> Node:
        """
        Create a folder.

        `parent_node` should be a folder you want to put this folder in.

        `folder_name` will be the name of the folder.

        `private` is an optional metadata, you can decide how to place this for
        each services.

        If `exist_ok` is `False`, you should not create the folder if it is
        already exists, and raise an exception.

        Will return the created node.
        """

    @abstractmethod
    async def rename_node(
        self,
        node: Node,
        *,
        new_parent: Optional[Node],
        new_name: Optional[str],
    ) -> Node:
        """
        Rename a node, or move to another folder, or do both.

        `node` is the node to be modified.

        `new_parent` is the new parent folder. `None` means don't move the node.

        `new_name` is the new node name. `None` means don't rename the node.
        """

    @abstractmethod
    async def trash_node(self, node: Node) -> None:
        """
        Trash the node.

        Should raise exception if failed.
        """

    @abstractmethod
    async def download(self, node: Node) -> ReadableFile:
        """
        Download the node.

        Will return a `ReadableFile` which is a file-like object.
        """

    @abstractmethod
    async def upload(
        self,
        parent_node: Node,
        file_name: str,
        *,
        file_size: Optional[int],
        mime_type: Optional[str],
        media_info: Optional[MediaInfo],
        private: Optional[PrivateDict],
    ) -> WritableFile:
        """
        Upload a file.

        `parent_node` is the target folder.

        `file_name` is required.

        `file_size` can be `None`, for cases that the file size is unavailable.
        e.g. The uploading file is from a stream.

        `mime_type`, `media_info` and `private` are optional. It is your choice
        to decide how to place these properties.
        """

    @abstractmethod
    async def get_hasher(self) -> Hasher:
        """
        Get a hash calculator.
        """

    @abstractmethod
    async def is_authorized(self) -> bool:
        """
        Is OAuth 2.0 authorized.
        """

    @abstractmethod
    async def get_oauth_url(self) -> str:
        """
        Get OAuth 2.0 URL.
        """

    @abstractmethod
    async def set_oauth_token(self, token: str) -> None:
        """
        Set OAuth 2.0 token.
        """


Middleware = RemoteDriver
