from typing import Optional, Pattern
import asyncio
import concurrent.futures
import functools
import pathlib
import re
import sqlite3

from .exceptions import CacheError
from .types import Node, ChangeDict


SQL_CREATE_TABLES = [
    """
    CREATE TABLE metadata (
        key TEXT NOT NULL,
        value TEXT,
        PRIMARY KEY (key)
    );
    """,
    """
    CREATE TABLE nodes (
        id TEXT NOT NULL,
        name TEXT,
        trashed BOOLEAN,
        created INTEGER,
        modified INTEGER,
        PRIMARY KEY (id)
    );
    """,
    "CREATE INDEX ix_nodes_names ON nodes(name);",
    "CREATE INDEX ix_nodes_trashed ON nodes(trashed);",
    "CREATE INDEX ix_nodes_created ON nodes(created);",
    "CREATE INDEX ix_nodes_modified ON nodes(modified);",
    """
    CREATE TABLE files (
        id TEXT NOT NULL,
        mime_type TEXT,
        hash TEXT,
        size INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    """,
    "CREATE INDEX ix_files_mime_type ON files(mime_type);",
    """
    CREATE TABLE parentage (
        parent TEXT NOT NULL,
        child TEXT NOT NULL,
        PRIMARY KEY (parent, child),
        FOREIGN KEY (parent) REFERENCES nodes (id),
        FOREIGN KEY (child) REFERENCES nodes (id)
    );
    """,
    "CREATE INDEX ix_parentage_parent ON parentage(parent);",
    "CREATE INDEX ix_parentage_child ON parentage(child);",
    """
    CREATE TABLE images (
        id TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    """,
    """
    CREATE TABLE videos (
        id TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        ms_duration INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    """,
    """
    CREATE TABLE private (
        id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT
    );
    """,
    "CREATE INDEX ix_private_id ON private(id);",
    "CREATE INDEX ix_private_key ON private(key);",
    "PRAGMA user_version = 4;",
]


CURRENT_SCHEMA_VERSION = 4


class Cache(object):
    def __init__(self, dsn: str, pool: concurrent.futures.Executor) -> None:
        self._dsn = dsn
        self._pool = pool
        self._raii = None

    async def __aenter__(self) -> "Cache":
        await self._bg(initialize)
        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        pass

    async def get_root_id(self) -> str:
        try:
            return await self.get_metadata("root_id")
        except KeyError:
            raise CacheError("invalid cache")

    async def get_root_node(self) -> Node:
        root_id = await self.get_root_id()
        return await self.get_node_by_id(root_id)

    async def get_metadata(self, key: str) -> str:
        return await self._bg(get_metadata, key)

    async def set_metadata(self, key: str, value: str) -> None:
        return await self._bg(set_metadata, key, value)

    async def get_node_by_id(self, node_id: str) -> Optional[Node]:
        return await self._bg(get_node_by_id, node_id)

    async def get_node_by_path(self, path: pathlib.PurePath) -> Optional[Node]:
        try:
            return await self._bg(get_node_by_path, path)
        except KeyError:
            raise CacheError("invalid cache")

    async def get_path_by_id(self, node_id: str) -> Optional[pathlib.PurePath]:
        return await self._bg(get_path_by_id, node_id)

    async def get_node_by_name_from_parent_id(
        self, name: str, parent_id: str
    ) -> Optional[Node]:
        return await self._bg(get_node_by_name_from_parent_id, name, parent_id)

    async def get_children_by_id(self, node_id: str) -> list[Node]:
        return await self._bg(get_children_by_id, node_id)

    async def get_trashed_nodes(self) -> list[Node]:
        return await self._bg(get_trashed_nodes)

    async def apply_changes(
        self,
        changes: list[ChangeDict],
        check_point: str,
    ) -> None:
        return await self._bg(apply_changes, changes, check_point)

    async def insert_node(self, node: Node) -> None:
        return await self._bg(insert_node, node)

    async def find_nodes_by_regex(self, pattern: str) -> list[Node]:
        return await self._bg(find_nodes_by_regex, pattern)

    async def get_uploaded_size(self, begin: int, end: int) -> int:
        return await self._bg(get_uploaded_size, begin, end)

    async def find_orphan_nodes(self) -> list[Node]:
        return await self._bg(find_orphan_nodes)

    async def find_multiple_parents_nodes(self) -> list[Node]:
        return await self._bg(find_multiple_parents_nodes)

    async def _bg(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, fn, self._dsn, *args)


class Database(object):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._db = None

    def __enter__(self) -> sqlite3.Connection:
        self._db = sqlite3.connect(self._dsn)
        self._db.row_factory = sqlite3.Row
        # self._db.execute('PRAGMA foreign_keys = 1;')
        return self._db

    def __exit__(self, type_, value, traceback) -> bool:
        self._db.close()
        self._db = None


class ReadOnly(object):
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def __enter__(self) -> sqlite3.Cursor:
        self._cursor = self._db.cursor()
        return self._cursor

    def __exit__(self, type_, value, traceback) -> bool:
        self._cursor.close()


class ReadWrite(object):
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def __enter__(self) -> sqlite3.Cursor:
        self._cursor = self._db.cursor()
        return self._cursor

    def __exit__(self, type_, value, traceback) -> bool:
        if self._db.in_transaction:
            if type_ is None:
                self._db.commit()
            else:
                self._db.rollback()
        self._cursor.close()


def initialize(dsn: str):
    with Database(dsn) as db:
        try:
            # initialize table
            with ReadWrite(db) as query:
                for sql in SQL_CREATE_TABLES:
                    query.execute(sql)
        except sqlite3.OperationalError as e:
            pass

        # check the schema version
        with ReadOnly(db) as query:
            query.execute("PRAGMA user_version;")
            rv = query.fetchone()
        version = int(rv[0])

        if version < CURRENT_SCHEMA_VERSION:
            raise CacheError(
                (
                    "impossible to migrate from old schema prior to "
                    f"version {CURRENT_SCHEMA_VERSION}, please rebuild the cache"
                )
            )


def get_metadata(dsn: str, key: str) -> str:
    with Database(dsn) as db, ReadOnly(db) as query:
        return inner_get_metadata(query, key)


def set_metadata(dsn: str, key: str, value: str) -> None:
    with Database(dsn) as db, ReadWrite(db) as query:
        inner_set_metadata(query, key, value)


def get_node_by_id(dsn: str, node_id: str) -> Optional[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        return inner_get_node_by_id(query, node_id)


def get_node_by_path(
    dsn: str,
    path: pathlib.PurePath,
) -> Optional[Node]:
    parts = path.parts[1:]
    with Database(dsn) as db, ReadOnly(db) as query:
        node_id = inner_get_metadata(query, "root_id")

        for part in parts:
            query.execute(
                """
                SELECT nodes.id AS id
                FROM parentage
                    INNER JOIN nodes ON parentage.child=nodes.id
                WHERE parentage.parent=? AND nodes.name=?
            ;""",
                (node_id, part),
            )
            rv = query.fetchone()
            if not rv:
                return None
            node_id = rv["id"]

        node = inner_get_node_by_id(query, node_id)
    return node


def get_path_by_id(dsn: str, node_id: str) -> Optional[pathlib.PurePath]:
    parts = []
    with Database(dsn) as db, ReadOnly(db) as query:
        while True:
            query.execute(
                """
                SELECT name
                FROM nodes
                WHERE id=?
            ;""",
                (node_id,),
            )
            rv = query.fetchone()
            if not rv:
                raise CacheError(f"cannot find name for {node_id}")

            name = rv["name"]
            if not name:
                parts.insert(0, "/")
                break
            parts.insert(0, name)

            query.execute(
                """
                SELECT parent
                FROM parentage
                WHERE child=?
            ;""",
                (node_id,),
            )
            rv = query.fetchone()
            if not rv:
                # orphan node
                break
            node_id = rv["parent"]

    path = pathlib.PurePath(*parts)
    return path


def get_node_by_name_from_parent_id(
    dsn: str, name: str, parent_id: str
) -> Optional[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        query.execute(
            """
            SELECT nodes.id AS id
            FROM nodes
                INNER JOIN parentage ON parentage.child=nodes.id
            WHERE parentage.parent=? AND nodes.name=?
        ;""",
            (parent_id, name),
        )
        rv = query.fetchone()

        if not rv:
            return None

        node = inner_get_node_by_id(query, rv["id"])
    return node


def get_children_by_id(dsn: str, node_id: str) -> list[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        query.execute(
            """
            SELECT child
            FROM parentage
            WHERE parent=?
        ;""",
            (node_id,),
        )
        rv = query.fetchall()

        children = [inner_get_node_by_id(query, _["child"]) for _ in rv]
    return children


def get_trashed_nodes(dsn: str) -> list[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        query.execute(
            """
            SELECT id
            FROM nodes
            WHERE trashed=?
        ;""",
            (True,),
        )
        rv = query.fetchall()

        children = [inner_get_node_by_id(query, _["id"]) for _ in rv]
    return children


def apply_changes(
    dsn: str,
    changes: list[ChangeDict],
    check_point: str,
) -> None:
    with Database(dsn) as db, ReadWrite(db) as query:
        for change in changes:
            is_removed = change["removed"]
            if is_removed:
                inner_delete_node_by_id(query, change["id"])
                continue

            node = Node.from_dict(change["node"])
            inner_insert_node(query, node)

        inner_set_metadata(query, "check_point", check_point)


def insert_node(dsn: str, node: Node) -> None:
    with Database(dsn) as db, ReadWrite(db) as query:
        inner_insert_node(query, node)

        if not node.name:
            inner_set_metadata(query, "root_id", node.id_)


def find_nodes_by_regex(dsn: str, pattern: str) -> list[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        fn = functools.partial(sqlite3_regexp, re.compile(pattern, re.I))
        db.create_function("REGEXP", 2, fn)
        query.execute("SELECT id FROM nodes WHERE name REGEXP ?;", (pattern,))
        rv = query.fetchall()
        rv = [inner_get_node_by_id(query, _["id"]) for _ in rv]
    return rv


def get_uploaded_size(dsn: str, begin: int, end: int) -> int:
    with Database(dsn) as db, ReadOnly(db) as query:
        query.execute(
            """
            SELECT SUM(size) AS sum
            FROM files
                INNER JOIN nodes ON files.id = nodes.id
            where created >= ? AND created < ?
        ;""",
            (begin, end),
        )
        rv = query.fetchone()
        if not rv:
            return 0
        if rv["sum"] is None:
            return 0
        return rv["sum"]


def find_orphan_nodes(dsn: str) -> list[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        query.execute(
            """
            SELECT nodes.id AS id
            FROM parentage
                LEFT OUTER JOIN nodes ON parentage.child=nodes.id
            WHERE parentage.parent IS NULL
        ;"""
        )
        rv = query.fetchall()
        rv = [inner_get_node_by_id(query, _["id"]) for _ in rv]
    return rv


def find_multiple_parents_nodes(dsn: str) -> list[Node]:
    with Database(dsn) as db, ReadOnly(db) as query:
        query.execute(
            """
            SELECT child, COUNT(child) AS parent_count
            FROM parentage
            GROUP BY child
            HAVING parent_count > 1
        ;"""
        )
        rv = query.fetchall()
        rv = [inner_get_node_by_id(query, _["child"]) for _ in rv]
    return rv


def inner_get_metadata(query: sqlite3.Cursor, key: str) -> str:
    query.execute("SELECT value FROM metadata WHERE key = ?;", (key,))
    rv = query.fetchone()
    if not rv:
        raise KeyError(key)
    return rv["value"]


def inner_set_metadata(query: sqlite3.Cursor, key: str, value: str) -> None:
    query.execute(
        """
        INSERT OR REPLACE INTO metadata
        VALUES (?, ?)
    ;""",
        (key, value),
    )


def inner_get_node_by_id(
    query: sqlite3.Cursor,
    node_id: str,
) -> Optional[Node]:
    query.execute(
        """
        SELECT name, trashed, created, modified
        FROM nodes
        WHERE id=?
    ;""",
        (node_id,),
    )
    rv = query.fetchone()
    if not rv:
        return None
    node = dict(rv)
    node["id"] = node_id

    query.execute(
        """
        SELECT mime_type, hash, size
        FROM files
        WHERE id=?
    ;""",
        (node_id,),
    )
    rv = query.fetchone()
    is_folder = rv is None
    node["is_folder"] = is_folder
    node["mime_type"] = None if is_folder else rv["mime_type"]
    node["hash"] = None if is_folder else rv["hash"]
    node["size"] = None if is_folder else rv["size"]

    query.execute(
        """
        SELECT parent
        FROM parentage
        WHERE child=?
    ;""",
        (node_id,),
    )
    rv = query.fetchall()
    node["parent_list"] = [_["parent"] for _ in rv]

    query.execute(
        """
        SELECT width, height
        FROM images
        WHERE id=?
    ;""",
        (node_id,),
    )
    rv = query.fetchone()
    node["image"] = (
        {
            "width": rv["width"],
            "height": rv["height"],
        }
        if rv
        else None
    )

    query.execute(
        """
        SELECT width, height, ms_duration
        FROM videos
        WHERE id=?
    ;""",
        (node_id,),
    )
    rv = query.fetchone()
    node["video"] = (
        {
            "width": rv["width"],
            "height": rv["height"],
            "ms_duration": rv["ms_duration"],
        }
        if rv
        else None
    )

    query.execute(
        """
        SELECT key, value
        FROM private
        WHERE id=?;
    """,
        (node_id,),
    )
    rv = query.fetchall()
    node["private"] = None if not rv else {_["key"]: _["value"] for _ in rv}

    node = Node.from_dict(node)
    return node


def inner_insert_node(query: sqlite3.Cursor, node: Node) -> None:
    # add this node
    query.execute(
        """
        INSERT OR REPLACE INTO nodes
        (id, name, trashed, created, modified)
        VALUES
        (?, ?, ?, ?, ?)
    ;""",
        (
            node.id_,
            node.name,
            node.trashed,
            node.created.int_timestamp,
            node.modified.int_timestamp,
        ),
    )

    # add file information
    if not node.is_folder:
        query.execute(
            """
            INSERT OR REPLACE INTO files
            (id, mime_type, hash, size)
            VALUES
            (?, ?, ?, ?)
        ;""",
            (node.id_, node.mime_type, node.hash_, node.size),
        )

    # remove old parentage
    query.execute(
        """
        DELETE FROM parentage
        WHERE child=?
    ;""",
        (node.id_,),
    )
    # add parentage if there is any
    if node.parent_list:
        for parent in node.parent_list:
            query.execute(
                """
                INSERT INTO parentage
                (parent, child)
                VALUES
                (?, ?)
            ;""",
                (parent, node.id_),
            )

    # add image information
    if node.is_image:
        query.execute(
            """
            INSERT OR REPLACE INTO images
            (id, width, height)
            VALUES
            (?, ?, ?)
        ;""",
            (node.id_, node.image_width, node.image_height),
        )

    # add video information
    if node.is_video:
        query.execute(
            """
            INSERT OR REPLACE INTO videos
            (id, width, height, ms_duration)
            VALUES
            (?, ?, ?, ?)
        ;""",
            (node.id_, node.video_width, node.video_height, node.video_ms_duration),
        )

    # remove old private
    query.execute(
        """
        DELETE FROM private
        WHERE id=?
    ;""",
        (node.id_,),
    )
    # add private information if any
    if node.private:
        for key, value in node.private.items():
            query.execute(
                """
                INSERT INTO private
                (id, key, value)
                VALUES
                (?, ?, ?)
            ;""",
                (node.id_, key, value),
            )


def inner_delete_node_by_id(query: sqlite3.Cursor, node_id: str) -> None:
    # remove from private
    query.execute(
        """
        DELETE FROM private
        WHERE id=?
    ;""",
        (node_id,),
    )

    # remove from videos
    query.execute(
        """
        DELETE FROM videos
        WHERE id=?
    ;""",
        (node_id,),
    )

    # remove from images
    query.execute(
        """
        DELETE FROM images
        WHERE id=?
    ;""",
        (node_id,),
    )

    # disconnect parents
    query.execute(
        """
        DELETE FROM parentage
        WHERE child=? OR parent=?
    ;""",
        (node_id, node_id),
    )

    # remove from files
    query.execute(
        """
        DELETE FROM files
        WHERE id=?
    ;""",
        (node_id,),
    )

    # remove from nodes
    query.execute(
        """
        DELETE FROM nodes
        WHERE id=?
    ;""",
        (node_id,),
    )


def sqlite3_regexp(
    pattern: Pattern,
    _: str,
    cell: Optional[str],
) -> bool:
    if cell is None:
        # root node
        return False
    return pattern.search(cell) is not None
