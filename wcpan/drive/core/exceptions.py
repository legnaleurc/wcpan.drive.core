from .types import Node


class DriveError(Exception):
    pass


class InvalidRemoteDriverError(DriveError):
    pass


class InvalidMiddlewareError(DriveError):
    pass


# NOTE *MUST* be picklable
class CacheError(DriveError):
    pass


class InvalidNameError(DriveError):

    def __init__(self, name: str) -> None:
        self._name = name

    def __str__(self) -> str:
        return f'invalid name: {self._name}'


class NodeTreeError(DriveError):
    pass


class NodeConflictedError(NodeTreeError):

    def __init__(self, node: Node) -> None:
        self._node = node

    def __str__(self) -> str:
        return f'node already exists: {self.node.name}'

    @property
    def node(self) -> Node:
        return self._node


class ParentNotFoundError(NodeTreeError):

    def __init__(self, id_: str) -> None:
        self._id = id_

    def __str__(self) -> str:
        return f'parent id not found: {self._id}'


class NodeNotFoundError(NodeTreeError):

    def __init__(self, path_or_id: str) -> None:
        self._path_or_id = path_or_id

    def __str__(self) -> str:
        return f'node not found: {self._path_or_id}'


class RootNodeError(NodeTreeError):
    pass


class TrashedNodeError(NodeTreeError):
    pass


class ParentIsNotFolderError(NodeTreeError):
    pass


class LineageError(NodeTreeError):
    pass


class UnauthorizedError(DriveError):
    pass


class DownloadError(DriveError):
    pass


class UploadError(DriveError):
    pass

