from typing import TypedDict, List, Optional, Literal, Union, Dict, Protocol
import os

import arrow


PathOrString = Union[str, os.PathLike]


class ImageDict(TypedDict):

    width: int
    height: int


class VideoDict(TypedDict):

    width: int
    height: int
    ms_duration: int


PrivateDict = Dict[str, str]


class NodeDict(TypedDict):

    id: str
    name: str
    trashed: bool
    created: str
    modified: str
    parent_list: List[str]
    is_folder: bool
    mime_type: Optional[str]
    hash: Optional[str]
    size: Optional[int]
    image: Optional[ImageDict]
    video: Optional[VideoDict]
    private: Optional[PrivateDict]


class Node(object):

    @staticmethod
    def from_dict(dict_: NodeDict) -> 'Node':
        return Node(
            id_=dict_['id'],
            name=dict_['name'],
            trashed=bool(dict_['trashed']),
            created=arrow.get(dict_['created']),
            modified=arrow.get(dict_['modified']),
            parent_list=dict_.get('parent_list', []),
            is_folder=dict_['is_folder'],
            mime_type=dict_['mime_type'],
            hash_=dict_['hash'],
            size=dict_['size'],
            image=dict_['image'],
            video=dict_['video'],
            private=dict_.get('private', None),
        )

    def __init__(self,
        *,
        id_: str,
        name: str,
        trashed: bool,
        created: arrow.Arrow,
        modified: arrow.Arrow,
        parent_list: List[str],
        is_folder: bool,
        mime_type: Optional[str],
        hash_: Optional[str],
        size: Optional[int],
        image: Optional[ImageDict],
        video: Optional[VideoDict],
        private: Optional[PrivateDict],
    ) -> None:
        self._id = id_
        self._name = name
        self._trashed = trashed
        self._created = created
        self._modified = modified
        self._parent_list = parent_list
        self._is_folder = is_folder
        self._mime_type = mime_type
        self._hash = hash_
        self._size = size
        self._image = image
        self._video = video
        self._private = private

    def __repr__(self):
        return f"Node(id='{self.id_}')"

    def __eq__(self, that: 'Node') -> bool:
        if not isinstance(that, Node):
            return NotImplemented
        return self.id_ == that.id_

    @property
    def is_root(self) -> bool:
        return self._name is None

    @property
    def id_(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def trashed(self) -> bool:
        return self._trashed

    @property
    def created(self) -> arrow.Arrow:
        return self._created

    @property
    def modified(self) -> arrow.Arrow:
        return self._modified

    @property
    def parent_list(self) -> List[str]:
        return self._parent_list

    @property
    def parent_id(self) -> str:
        return None if not self._parent_list else self._parent_list[0]

    @property
    def is_file(self) -> bool:
        return not self._is_folder

    @property
    def is_folder(self) -> bool:
        return self._is_folder

    @property
    def mime_type(self) -> Optional[str]:
        return self._mime_type

    @property
    def hash_(self) -> Optional[str]:
        return self._hash

    @property
    def size(self) -> Optional[int]:
        return self._size

    @property
    def is_image(self) -> bool:
        return self._image is not None

    @property
    def image_width(self) -> Optional[int]:
        return self._image['width'] if self.is_image else None

    @property
    def image_height(self) -> Optional[int]:
        return self._image['height'] if self.is_image else None

    @property
    def is_video(self) -> bool:
        return self._video is not None

    @property
    def video_width(self) -> Optional[int]:
        return self._video['width'] if self.is_video else None

    @property
    def video_height(self) -> Optional[int]:
        return self._video['height'] if self.is_video else None

    @property
    def video_ms_duration(self) -> Optional[int]:
        return self._video['ms_duration'] if self.is_video else None

    @property
    def private(self) -> Optional[PrivateDict]:
        return self._private

    def clone(self,
        *,
        name: str = None,
        trashed: bool = None,
        created: arrow.Arrow = None,
        modified: arrow.Arrow = None,
        parent_list: List[str] = None,
        is_folder: bool = None,
        mime_type: str = None,
        hash_: str = None,
        size: int = None,
        image: ImageDict = None,
        video: VideoDict = None,
        private: PrivateDict = None,
    ) -> 'Node':
        return Node(
            id_=self.id_,
            name=self.name if name is None else name,
            trashed=self.trashed if trashed is None else trashed,
            created=self.created if created is None else created,
            modified=self.modified if modified is None else modified,
            parent_list=self.parent_list if parent_list is None else parent_list,
            is_folder=self.is_folder if is_folder is None else is_folder,
            mime_type=self.mime_type if mime_type is None else mime_type,
            hash_=self.hash_ if hash_ is None else hash_,
            size=self.size if size is None else size,
            image=self._image if image is None else image,
            video=self._video if video is None else video,
            private=self.private if private is None else private,
        )

    def to_dict(self) -> NodeDict:
        dict_ = {
            'id': self.id_,
            'name': self.name,
            'trashed': self.trashed,
            'is_folder': self.is_folder,
            'created': self.created.isoformat(),
            'modified': self.modified.isoformat(),
            'parent_list': self.parent_list.copy(),
            'mime_type': self.mime_type,
            'hash': self.hash_,
            'size': self.size,
        }
        if not self.is_image:
            dict_['image'] = None
        else:
            dict_['image'] = {
                'width': self.image_width,
                'height': self.image_height,
            }
        if not self.is_video:
            dict_['video'] = None
        else:
            dict_['video'] = {
                'width': self.video_width,
                'height': self.video_height,
                'ms_duration': self.video_ms_duration,
            }
        if not self.private:
            dict_['private'] = None
        else:
            dict_['private'] = self.private.copy()
        return dict_


class RemoveChangeDict(TypedDict):

    removed: Literal[True]
    id: str


class UpdateChangeDict(TypedDict):

    removed: Literal[False]
    node: NodeDict


ChangeDict = Union[RemoveChangeDict, UpdateChangeDict]


class MediaInfo(object):

    @staticmethod
    def image(width: int, height: int) -> 'MediaInfo':
        return MediaInfo(is_image=True, width=width, height=height)

    @staticmethod
    def video(width: int, height: int, ms_duration: int) -> 'MediaInfo':
        return MediaInfo(
            is_video=True,
            width=width,
            height=height,
            ms_duration=ms_duration,
        )

    def __init__(self,
        *,
        is_image: bool = False,
        is_video: bool = False,
        width: int = 0,
        height: int = 0,
        ms_duration: int = 0,
    ) -> None:
        self._is_image = is_image
        self._is_video = is_video
        self._width = width
        self._height = height
        self._ms_duration = ms_duration

    @property
    def is_image(self) -> bool:
        return self._is_image

    @property
    def is_video(self) -> bool:
        return self._is_video

    @property
    def width(self) -> int:
        return self._width

    @property
    def height(self) -> int:
        return self._height

    @property
    def ms_duration(self) -> int:
        return self._ms_duration


class RenameNodeFunction(Protocol):

    async def __call__(self,
        node: Node,
        new_parent: Optional[Node],
        new_name: Optional[str],
    ) -> Node:
        pass


class DownloadFunction(Protocol):

    async def __call__(self, node: Node) -> 'ReadableFile':
        pass


class UploadFunction(Protocol):

    async def __call__(self,
        parent_node: Node,
        file_name: str,
        file_size: Optional[int],
        mime_type: Optional[str],
        media_info: Optional[MediaInfo],
        private: Optional[PrivateDict],
    ) -> 'WritableFile':
        pass


class CreateFolderFunction(Protocol):

    async def __call__(self,
        parent_node: Node,
        folder_name: str,
        private: Optional[PrivateDict],
        exist_ok: bool,
    ) -> Node:
        pass


class GetHasherFunction(Protocol):

    async def __call__(self) -> 'Hasher':
        pass
