import contextlib
import unittest

from wcpan.drive.core.drive import Drive
from wcpan.drive.core.exceptions import (
    CacheError,
    LineageError,
    NodeConflictedError,
    NodeNotFoundError,
    ParentIsNotFolderError,
    RootNodeError,
    TrashedNodeError,
    DownloadError,
)
from wcpan.drive.core.test import test_factory, TestDriver


class TestDrive(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async with contextlib.AsyncExitStack() as stack:
            factory = stack.enter_context(
                test_factory(
                    middleware_list=[
                        "tests.driver.FakeMiddleware",
                        "tests.driver.FakeMiddleware",
                    ]
                )
            )
            self._drive: Drive = await stack.enter_async_context(factory())
            self._driver: TestDriver = self._drive.remote.remote.remote

            self._raii = stack.pop_all()

    async def asyncTearDown(self):
        await self._raii.aclose()
        self._driver = None
        self._drive = None
        self._raii = None

    async def testSync(self):
        driver = self._driver
        root_node = await driver.fetch_root_node()

        # normal file and folder
        builder = driver.pseudo.build_node()
        builder.to_folder("name_1", root_node)
        node_1 = builder.commit()
        builder = driver.pseudo.build_node()
        builder.to_folder("name_2", node_1)
        builder.to_file(2, "hash_2", "text/plain")
        node_2 = builder.commit()

        pseudo_changes = driver.pseudo.changes
        applied_changes = [change async for change in self._drive.sync()]
        self.assertEqual(applied_changes, pseudo_changes)

        node = await self._drive.get_node_by_path("/name_1")
        self.assertIsNotNone(node)
        self.assertTrue(node.is_folder)
        self.assertEqual(node.id_, node_1.id_)
        node = await self._drive.get_node_by_path("/name_1/name_2")
        self.assertIsNotNone(node)
        self.assertTrue(node.is_file)
        self.assertEqual(node.id_, node_2.id_)
        self.assertEqual(node.size, 2)
        self.assertEqual(node.hash_, "hash_2")
        self.assertEqual(node.mime_type, "text/plain")

        # TODO
        # # atomic
        # builder.reset()
        # builder.update(create_file(
        #     'id_3',
        #     'name_3',
        #     'id_1',
        #     3,
        #     'hash_3',
        #     'text/plain',
        # ))
        # driver.set_changes('3', builder.changes)

        # with self.assertRaises(Exception):
        #     async for change in self._drive.sync():
        #         raise Exception('interrupt')

        # node = await self._drive.get_node_by_path('/name_1/name_3')
        # self.assertIsNone(node)

        # image and video
        builder = driver.pseudo.build_node()
        builder.to_folder("name_4", node_1)
        builder.to_file(4, "hash_4", "image/png")
        builder.to_image(640, 480)
        node_4 = builder.commit()
        builder = driver.pseudo.build_node()
        builder.to_folder("name_5", node_1)
        builder.to_file(5, "hash_5", "video/mpeg")
        builder.to_video(640, 480, 5)
        node_5 = builder.commit()

        pseudo_changes = driver.pseudo.changes
        applied_changes = [change async for change in self._drive.sync()]
        self.assertEqual(applied_changes, pseudo_changes)

        node = await self._drive.get_node_by_path("/name_1/name_4")
        self.assertTrue(node.is_image)
        self.assertEqual(node.image_width, 640)
        self.assertEqual(node.image_height, 480)
        node = await self._drive.get_node_by_path("/name_1/name_5")
        self.assertTrue(node.is_video)
        self.assertEqual(node.video_width, 640)
        self.assertEqual(node.video_height, 480)
        self.assertEqual(node.video_ms_duration, 5)

        # delete file
        driver.pseudo.delete(node_2.id_)

        pseudo_changes = driver.pseudo.changes
        applied_changes = [change async for change in self._drive.sync()]
        self.assertEqual(applied_changes, pseudo_changes)

        node = await self._drive.get_node_by_path("/name_1/name_2")
        self.assertIsNone(node)

        # delete folder
        driver.pseudo.delete(node_1.id_)

        pseudo_changes = driver.pseudo.changes
        applied_changes = [change async for change in self._drive.sync()]
        self.assertEqual(applied_changes, pseudo_changes)

        node = await self._drive.get_node_by_path("/name_1")
        self.assertIsNone(node)
        node = await self._drive.get_node_by_id(node_4.id_)
        self.assertIsNone(node.parent_id)
        node = await self._drive.get_node_by_id(node_5.id_)
        self.assertIsNone(node.parent_id)

    async def testCreateFolder(self):
        driver = self._driver
        mock = driver.mock.create_folder
        api = self._drive.create_folder
        root_node = await driver.fetch_root_node()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_1", root_node)
        builder.to_file(1, "hash_1", "text/plain")
        builder.commit()

        async for dummy_change in self._drive.sync():
            pass

        # invalid parent
        with self.assertRaises(TypeError):
            await api(None, "name")
        mock.assert_not_called()
        mock.reset_mock()

        # invalid name
        with self.assertRaises(TypeError):
            await api(root_node, None)
        mock.assert_not_called()
        mock.reset_mock()

        # invalid parent
        node = await self._drive.get_node_by_path("/name_1")
        self.assertIsNotNone(node)
        with self.assertRaises(ParentIsNotFolderError):
            await api(node, "invalid")
        mock.assert_not_called()
        mock.reset_mock()

        # conflict
        with self.assertRaises(NodeConflictedError):
            await api(root_node, "name_1")
        mock.assert_not_called()
        mock.reset_mock()

        # good calls
        await api(root_node, "name")
        mock.assert_called_once_with(
            root_node,
            "name",
            None,
            False,
        )
        mock.reset_mock()

        await api(root_node, "name", True)
        mock.assert_called_once_with(
            root_node,
            "name",
            None,
            True,
        )
        mock.reset_mock()

    async def testRenameNode(self):
        driver = self._driver
        mock = driver.mock.rename_node
        api = self._drive.rename_node
        api_alt = self._drive.rename_node_by_path
        root_node = await driver.fetch_root_node()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_1", root_node)
        node_1 = builder.commit()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_2", node_1)
        builder.to_file(2, "hash_2", "text/plain")
        builder.commit()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_3", node_1)
        builder.commit()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_4", node_1)
        builder.to_file(4, "hash_4", "text/plain")
        builder.commit()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_5", node_1)
        builder.to_trashed()
        node_5 = builder.commit()

        async for dummy_change in self._drive.sync():
            pass

        # source is not None
        with self.assertRaises(TypeError):
            await api(None, root_node)
        mock.assert_not_called()
        mock.reset_mock()

        # at least have a new parent or new name
        node = await self._drive.get_node_by_path("/name_1")
        with self.assertRaises(TypeError):
            await api(node, None, None)
        mock.assert_not_called()
        mock.reset_mock()

        # do not touch trash can
        node1 = await self._drive.get_node_by_path("/name_1")
        node2 = await self._drive.get_node_by_id(node_5.id_)
        with self.assertRaises(TrashedNodeError):
            await api(node1, node2, None)
        with self.assertRaises(TrashedNodeError):
            await api(node2, node1, None)
        mock.assert_not_called()
        mock.reset_mock()

        # do not move to the source folder
        node = await self._drive.get_node_by_path("/name_1")
        with self.assertRaises(LineageError):
            await api(node, node, None)
        mock.assert_not_called()
        mock.reset_mock()

        # do not move root
        with self.assertRaises(RootNodeError):
            await api_alt("/", "/name_1")
        mock.assert_not_called()
        mock.reset_mock()

        # do not overwrite file
        with self.assertRaises(NodeConflictedError):
            await api_alt("/name_1/name_2", "/name_1/name_4")
        mock.assert_not_called()
        mock.reset_mock()

        # do not move parent to descendant folders
        with self.assertRaises(LineageError):
            await api_alt("/name_1", "/name_1/name_3")
        mock.assert_not_called()
        mock.reset_mock()

        # do not move invalid node
        with self.assertRaises(NodeNotFoundError):
            await api_alt("/invalid", "/name_1")
        mock.assert_not_called()
        mock.reset_mock()

        # do not move to invalid path
        with self.assertRaises(LineageError):
            await api_alt("/name_1/name_2", "/invalid/invalid")
        with self.assertRaises(LineageError):
            await api_alt("/name_1/name_2", "./invalid/invalid")
        mock.assert_not_called()
        mock.reset_mock()

        # move to absolute path
        node = await self._drive.get_node_by_path("/name_1")
        await api_alt("/name_1", "/new")
        mock.assert_called_once_with(node, root_node, "new")
        mock.reset_mock()

        # move to relative folder
        node1 = await self._drive.get_node_by_path("/name_1/name_2")
        node2 = await self._drive.get_node_by_path("/name_1/name_3")
        await api_alt("/name_1/name_2", "./name_3")
        mock.assert_called_once_with(node1, node2, None)
        mock.reset_mock()

        # move to relative file
        node1 = await self._drive.get_node_by_path("/name_1/name_2")
        node2 = await self._drive.get_node_by_path("/name_1/name_3")
        await api_alt("/name_1/name_2", "./name_3/name_4")
        mock.assert_called_once_with(node1, node2, "name_4")
        mock.reset_mock()

        # move up
        node = await self._drive.get_node_by_path("/name_1/name_2")
        await api_alt("/name_1/name_2", "..")
        mock.assert_called_once_with(node, root_node, None)
        mock.reset_mock()

    async def testTrashNode(self):
        driver = self._driver
        mock = driver.mock.trash_node
        api = self._drive.trash_node
        root_node = await driver.fetch_root_node()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_1", root_node)
        builder.commit()

        async for dummy_change in self._drive.sync():
            pass

        # do not trash root node
        with self.assertRaises(RootNodeError):
            await api(root_node)
        mock.assert_not_called()
        mock.reset_mock()

        # do not accept invalid node
        with self.assertRaises(TypeError):
            await api(None)
        mock.assert_not_called()
        mock.reset_mock()

        # good call
        node = await self._drive.get_node_by_path("/name_1")
        await api(node)
        mock.assert_called_once_with(node)
        mock.reset_mock()

    async def testDownload(self):
        driver = self._driver
        mock = driver.mock.download
        api = self._drive.download
        root_node = await driver.fetch_root_node()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_1", root_node)
        node_1 = builder.commit()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_2", node_1)
        builder.to_file(2, "hash_2", "text/plain")
        builder.commit()

        async for dummy_change in self._drive.sync():
            pass

        # do not download folder
        node = await self._drive.get_node_by_path("/name_1")
        with self.assertRaises(DownloadError):
            await api(node)
        mock.assert_not_called()
        mock.reset_mock()

        # do not accept invalid node
        with self.assertRaises(TypeError):
            await api(None)
        mock.assert_not_called()
        mock.reset_mock()

        # good call
        node = await self._drive.get_node_by_path("/name_1/name_2")
        await api(node)
        mock.assert_called_once_with(node)
        mock.reset_mock()

    async def testUpload(self):
        driver = self._driver
        mock = driver.mock.upload
        api = self._drive.upload
        root_node = await driver.fetch_root_node()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_1", root_node)
        node_1 = builder.commit()

        builder = driver.pseudo.build_node()
        builder.to_folder("name_2", node_1)
        builder.to_file(2, "hash_2", "text/plain")
        builder.commit()

        async for dummy_change in self._drive.sync():
            pass

        # do not upload to a file
        node = await self._drive.get_node_by_path("/name_1/name_2")
        with self.assertRaises(ParentIsNotFolderError):
            await api(node, "name_3")
        mock.assert_not_called()
        mock.reset_mock()

        # do not conflict
        node = await self._drive.get_node_by_path("/name_1")
        with self.assertRaises(NodeConflictedError):
            await api(node, "name_2")
        mock.assert_not_called()
        mock.reset_mock()

        # do not accept invalid node
        with self.assertRaises(TypeError):
            await api(None, "name_3")
        mock.assert_not_called()
        mock.reset_mock()

        # do not accept invalid name
        with self.assertRaises(TypeError):
            await api(node, None)
        mock.assert_not_called()
        mock.reset_mock()

        # good calls
        await api(node, "name_3")
        mock.assert_called_once_with(node, "name_3", None, None, None, None)
        mock.reset_mock()

        await api(node, "name_3", file_size=123, mime_type="test/plain")
        mock.assert_called_once_with(
            node,
            "name_3",
            123,
            "test/plain",
            None,
            None,
        )
        mock.reset_mock()

    async def testGetHasher(self):
        driver = self._driver
        mock = driver.mock.get_hasher

        await self._drive.get_hasher()
        mock.assert_called_once_with()
        mock.reset_mock()

    async def testEmptyCache(self):
        with self.assertRaises(CacheError):
            await self._drive.get_root_node()

        node = await self._drive.get_node_by_id("not_exist")
        self.assertIsNone(node)

        with self.assertRaises(CacheError):
            await self._drive.get_node_by_path("/")

        node = await self._drive.get_node_by_name_from_parent_id(
            "not_exist", "not_exist"
        )
        self.assertIsNone(node)

        node_list = await self._drive.get_children_by_id("not_exist")
        self.assertFalse(node_list)

        node_list = await self._drive.get_trashed_nodes()
        self.assertFalse(node_list)
