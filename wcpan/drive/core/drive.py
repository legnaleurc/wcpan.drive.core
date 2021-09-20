__all__ = ('Drive', 'DriveFactory')


from typing import List, AsyncGenerator, Tuple, Optional, Type, Union
import asyncio
import concurrent.futures
import contextlib
import functools
import pathlib

import yaml

from .cache import Cache
from .exceptions import (
    DownloadError,
    InvalidMiddlewareError,
    InvalidRemoteDriverError,
    LineageError,
    NodeConflictedError,
    NodeNotFoundError,
    ParentIsNotFolderError,
    RootNodeError,
    TrashedNodeError,
)
from .types import (
    ChangeDict,
    MediaInfo,
    Node,
    PathOrString,
    ReadOnlyContext,
)
from .abc import ReadableFile, WritableFile, Hasher, RemoteDriver, Middleware
from .util import (
    create_executor,
    get_default_config_path,
    get_default_data_path,
    import_class,
    normalize_path,
    resolve_path,
)


DRIVER_VERSION = 1


class PrivateContext(object):

    def __init__(self,
        config_path: pathlib.Path,
        data_path: pathlib.Path,
        database_dsn: str,
        driver_class: Type[RemoteDriver],
        middleware_class_list: List[Type[Middleware]],
        pool: Optional[concurrent.futures.Executor],
    ) -> None:
        self._context = ReadOnlyContext(
            config_path=config_path,
            data_path=data_path,
        )
        self._database_dsn = database_dsn
        self._driver_class = driver_class
        self._middleware_class_list = middleware_class_list
        self._pool = pool

    @property
    def database_dsn(self):
        return self._database_dsn

    @property
    def pool(self) -> Optional[concurrent.futures.Executor]:
        return self._pool

    def create_remote_driver(self) -> RemoteDriver:
        driver = functools.reduce(
            lambda driver, class_: class_(self._context, driver),
            # bottom-most is the inner-most middleware
            reversed(self._middleware_class_list),
            self._driver_class(self._context),
        )
        return driver


class Drive(object):
    '''Interact with the drive.

    Please use DriveFactory to create an instance.

    The core module DOES NOT provide ANY implementation for cloud drive by
    itself. You need a driver class, which can be set in DriveFactory.
    '''

    def __init__(self, context: PrivateContext) -> None:
        self._context = context
        self._sync_lock = asyncio.Lock()

        self._remote: Union[RemoteDriver, None] = None

        self._pool = None
        self._db: Union[Cache, None] = None

        self._raii = None

    async def __aenter__(self) -> 'Drive':
        async with contextlib.AsyncExitStack() as stack:
            if not self._context.pool:
                self._pool = stack.enter_context(create_executor())
            else:
                self._pool = self._context.pool

            self._remote = await stack.enter_async_context(
                self._context.create_remote_driver()
            )

            dsn = self._context.database_dsn
            self._db = await stack.enter_async_context(Cache(dsn, self._pool))

            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._raii.aclose()
        self._remote = None
        self._pool = None
        self._db = None
        self._raii = None

    async def get_root_node(self) -> Node:
        '''Get the root node.'''
        return await self._db.get_root_node()

    async def get_node_by_id(self, node_id: str) -> Node:
        '''Get node by node id.'''
        return await self._db.get_node_by_id(node_id)

    async def get_node_by_path(self, path: PathOrString) -> Optional[Node]:
        '''Get node by absolute path.'''
        path = pathlib.PurePath(path)
        path = normalize_path(path)
        return await self._db.get_node_by_path(path)

    async def get_path(self, node: Node) -> Optional[pathlib.PurePath]:
        '''Get absolute path of the node.'''
        return await self._db.get_path_by_id(node.id_)

    async def get_path_by_id(self, node_id: str) -> str:
        '''Get absolute path of the node id.'''
        return await self._db.get_path_by_id(node_id)

    async def get_node_by_name_from_parent_id(self,
        name: str,
        parent_id: str,
    ) -> Node:
        '''Get node by given name and parent id.'''
        return await self._db.get_node_by_name_from_parent_id(name, parent_id)

    async def get_node_by_name_from_parent(self,
        name: str,
        parent: Node,
    ) -> Node:
        '''Get node by given name and parent node.'''
        return await self._db.get_node_by_name_from_parent_id(name, parent.id_)

    async def get_children(self, node: Node) -> List[Node]:
        '''Get the child node list of given node.'''
        return await self._db.get_children_by_id(node.id_)

    async def get_children_by_id(self, node_id: str) -> List[Node]:
        '''Get the child node list of given node id.'''
        return await self._db.get_children_by_id(node_id)

    async def get_trashed_nodes(self) -> List[Node]:
        '''Get trashed node list.'''
        return await self._db.get_trashed_nodes()

    async def find_nodes_by_regex(self, pattern: str) -> List[Node]:
        '''Find nodes by name.'''
        return await self._db.find_nodes_by_regex(pattern)

    async def find_orphan_nodes(self) -> List[Node]:
        '''Find nodes which are dangling from root.'''
        return await self._db.find_orphan_nodes()

    async def find_multiple_parents_nodes(self) -> List[Node]:
        '''Find nodes which have two or more parents.'''
        return await self._db.find_multiple_parents_nodes()

    async def walk(self,
        node: Node,
        include_trashed: bool = False,
    ) -> AsyncGenerator[Tuple[Node, List[Node], List[Node]], None]:
        '''Traverse nodes from given node.'''
        if not node.is_folder:
            return
        q = [node]
        while q:
            node = q[0]
            del q[0]
            if not include_trashed and node.trashed:
                continue

            children = await self.get_children(node)
            folders = []
            files = []
            for child in children:
                if not include_trashed and child.trashed:
                    continue
                if child.is_folder:
                    folders.append(child)
                else:
                    files.append(child)
            yield node, folders, files

            q.extend(folders)

    async def create_folder(self,
        parent_node: Node,
        folder_name: str,
        exist_ok: bool = False,
    ) -> Node:
        '''Create a folder.'''
        # sanity check
        if not self._remote:
            raise InvalidRemoteDriverError()
        if not parent_node:
            raise TypeError('invalid parent node')
        if not parent_node.is_folder:
            raise ParentIsNotFolderError('invalid parent node')
        if not folder_name:
            raise TypeError('invalid folder name')

        if not exist_ok:
            node = await self.get_node_by_name_from_parent(
                folder_name,
                parent_node,
            )
            if node:
                raise NodeConflictedError(node)

        return await self._remote.create_folder(
            parent_node=parent_node,
            folder_name=folder_name,
            private=None,
            exist_ok=exist_ok,
        )

    async def download_by_id(self, node_id: str) -> ReadableFile:
        '''Download the node.'''
        node = await self.get_node_by_id(node_id)
        return await self.download(node)

    async def download(self, node: Node) -> ReadableFile:
        '''Download the node.'''
        # sanity check
        if not self._remote:
            raise InvalidRemoteDriverError()
        if not node:
            raise TypeError('node is none')
        if node.is_folder:
            raise DownloadError('node should be a file')

        return await self._remote.download(node)

    async def upload_by_id(self,
        parent_id: str,
        file_name: str,
        *,
        file_size: int = None,
        mime_type: str = None,
        media_info: MediaInfo = None,
    ) -> WritableFile:
        '''Upload file.'''
        parent_node = await self.get_node_by_id(parent_id)
        return await self.upload(
            parent_node,
            file_name,
            file_size=file_size,
            mime_type=mime_type,
            media_info=media_info,
        )

    async def upload(self,
        parent_node: Node,
        file_name: str,
        *,
        file_size: int = None,
        mime_type: str = None,
        media_info: MediaInfo = None,
    ) -> WritableFile:
        '''Upload file.'''
        # sanity check
        if not self._remote:
            raise InvalidRemoteDriverError()
        if not parent_node:
            raise TypeError('invalid parent node')
        if not parent_node.is_folder:
            raise ParentIsNotFolderError('invalid parent node')
        if not file_name:
            raise TypeError('invalid file name')

        node = await self.get_node_by_name_from_parent(file_name, parent_node)
        if node:
            raise NodeConflictedError(node)

        return await self._remote.upload(
            parent_node,
            file_name,
            file_size=file_size,
            mime_type=mime_type,
            media_info=media_info,
            private=None,
        )

    async def trash_node_by_id(self, node_id: str) -> None:
        '''Move the node to trash.'''
        node = await self.get_node_by_id(node_id)
        await self.trash_node(node)

    async def trash_node(self, node: Node) -> None:
        '''Move the node to trash.'''
        # sanity check
        if not self._remote:
            raise InvalidRemoteDriverError()
        if not node:
            raise TypeError('source node is none')
        root_node = await self.get_root_node()
        if root_node.id_ == node.id_:
            raise RootNodeError('cannot trash root node')
        await self._remote.trash_node(node)

    async def rename_node(self,
        node: Node,
        new_parent: Node = None,
        new_name: str = None,
    ) -> Node:
        '''Move or rename the node.'''
        # sanity check
        if not self._remote:
            raise InvalidRemoteDriverError()
        if not node:
            raise TypeError('source node is none')
        if node.trashed:
            raise TrashedNodeError('source node is in trash')
        root_node = await self.get_root_node()
        if node.id_ == root_node.id_:
            raise RootNodeError('source node is the root node')

        if not new_parent and not new_name:
            raise TypeError('need new_parent or new_name')

        if new_parent:
            if new_parent.trashed:
                raise TrashedNodeError('new_parent is in trash')
            if new_parent.is_file:
                raise ParentIsNotFolderError('new_parent is not a folder')
            ancestor = new_parent
            while True:
                if ancestor.id_ == node.id_:
                    raise LineageError('new_parent is a descendant of node')
                if not ancestor.parent_id:
                    break
                ancestor = await self.get_node_by_id(ancestor.parent_id)

        return await self._remote.rename_node(
            node=node,
            new_parent=new_parent,
            new_name=new_name,
        )

    async def rename_node_by_id(self,
        node_id: str,
        new_parent_id: str = None,
        new_name: str = None,
    ) -> Node:
        '''Move or rename the node.'''
        node = await self.get_node_by_id(node_id)
        new_parent = await self.get_node_by_id(new_parent_id)
        return await self.rename_node(node, new_parent, new_name)

    async def rename_node_by_path(self,
        src_path: PathOrString,
        dst_path: PathOrString,
    ) -> Node:
        '''
        Rename or move `src_path` to `dst_path`. `dst_path` can be a file name
        or an absolute path.

        If `dst_path` is a file and already exists, `NodeConflictedError` will
        be raised.

        If `dst_path` is a folder, `src_path` will be moved to there without
        renaming.

        If `dst_path` does not exist yet, `src_path` will be moved and rename to
        `dst_path`.
        '''
        node = await self.get_node_by_path(src_path)
        if not node:
            raise NodeNotFoundError(src_path)

        src_path = str(src_path)
        dst_path = str(dst_path)
        src = pathlib.PurePath(src_path)
        dst = pathlib.PurePath(dst_path)

        # case 1 - move to a relative path
        if not dst.is_absolute():
            # case 1.1 - a name, not path
            if dst.name == dst_path:
                # case 1.1.1 - move to the same folder, do nothing
                if dst.name == '.':
                    return node
                # case 1.1.2 - rename only
                if dst.name != '..':
                    return await self.rename_node(node, None, dst.name)
                # case 1.1.3 - move to parent folder, the same as case 1.2

            # case 1.2 - a relative path, resolve to absolute path
            # NOTE pathlib.PurePath does not implement normalizing algorithm
            dst = resolve_path(src.parent, dst)

        # case 2 - move to an absolute path
        dst_node = await self.get_node_by_path(str(dst))
        # case 2.1 - the destination is empty
        if not dst_node:
            # move to the parent folder of the destination
            new_parent = await self.get_node_by_path(str(dst.parent))
            if not new_parent:
                raise LineageError(f'no direct path to {dst_path}')
            return await self.rename_node(node, new_parent, dst.name)
        # case 2.2 - the destination is a file
        if dst_node.is_file:
            # do not overwrite existing file
            raise NodeConflictedError(dst_node)
        # case 2.3 - the distination is a folder
        return await self.rename_node(node, dst_node, None)

    async def sync(self,
        check_point: str = None,
    ) -> AsyncGenerator[ChangeDict, None]:
        '''Synchronize the local node cache.

        This is the ONLY function which will modify the local cache.
        '''
        if not self._remote:
            raise InvalidRemoteDriverError()
        async with self._sync_lock:
            dry_run = check_point is not None
            initial_check_point = await self._remote.get_initial_check_point()

            if not dry_run:
                try:
                    check_point = await self._db.get_metadata('check_point')
                except KeyError:
                    check_point = initial_check_point

            # no data before, get the root node and cache it
            if not dry_run and check_point == initial_check_point:
                node = await self._remote.fetch_root_node()
                await self._db.insert_node(node)

            async for next_, changes in self._remote.fetch_changes(check_point):
                if not dry_run:
                    await self._db.apply_changes(changes, next_)

                for change in changes:
                    yield change

    async def get_hasher(self) -> Hasher:
        '''Get a Hasher instance for checksum calculation.'''
        if not self._remote:
            raise InvalidRemoteDriverError()
        return await self._remote.get_hasher()


class DriveFactory(object):

    def __init__(self) -> None:
        self._config_path = get_default_config_path()
        self._data_path = get_default_data_path()
        self.database = None
        self.driver = None
        self.middleware_list = []

    @property
    def config_path(self) -> pathlib.Path:
        '''The path which contains config files.'''
        return self._config_path

    @config_path.setter
    def config_path(self, path: PathOrString) -> None:
        self._config_path = pathlib.Path(path)

    @property
    def data_path(self) -> pathlib.Path:
        '''The path which contains data files.'''
        return self._data_path

    @data_path.setter
    def data_path(self, path: PathOrString) -> None:
        self._data_path = pathlib.Path(path)

    def load_config(self) -> None:
        '''The path which contains data files.'''
        # ensure we can access the folder
        self.config_path.mkdir(parents=True, exist_ok=True)

        config_file_path = self.config_path / 'core.yaml'

        with config_file_path.open('r') as fin:
            config_dict = yaml.safe_load(fin)

        for key in ('version', 'database', 'driver', 'middleware'):
            if key not in config_dict:
                raise ValueError(f'no required key: {key}')

        self.database = config_dict['database']
        self.driver = config_dict['driver']
        self.middleware_list = config_dict['middleware']

    def __call__(self, pool: concurrent.futures.Executor = None) -> Drive:
        # ensure we can access the folders
        self.config_path.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)

        # TODO use real dsn
        path = pathlib.Path(self.database)
        if not path.is_absolute():
            path = self.data_path / path
        dsn = str(path)

        driver_class = import_class(self.driver)
        min_, max_ = driver_class.get_version_range()
        if not min_ <= DRIVER_VERSION <= max_:
            raise InvalidRemoteDriverError()

        middleware_class_list = []
        for middleware in self.middleware_list:
            middleware_class = import_class(middleware)
            min_, max_ = middleware_class.get_version_range()
            if not min_ <= DRIVER_VERSION <= max_:
                raise InvalidMiddlewareError()
            middleware_class_list.append(middleware_class)

        context = PrivateContext(
            config_path=self.config_path,
            data_path=self.data_path,
            database_dsn=dsn,
            driver_class=driver_class,
            middleware_class_list=middleware_class_list,
            pool=pool,
        )
        return Drive(context)
