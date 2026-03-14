Quick Start
===========

Installation
------------

.. code-block:: sh

   pip install wcpan-drive-core

This package provides the core abstractions only. You also need a
:class:`~wcpan.drive.core.types.FileService` implementation for your cloud
provider and a :class:`~wcpan.drive.core.types.SnapshotService` implementation
for local metadata caching.

Basic Usage
-----------

.. code-block:: python

   from wcpan.drive.core import create_drive
   from wcpan.drive.core.lib import download_file_to_local, upload_file_from_local

   async with create_drive(
       file=create_my_file_service,
       snapshot=create_my_snapshot_service,
   ) as drive:
       # Authenticate if needed
       if not await drive.is_authenticated():
           await drive.authenticate()

       # Keep the local snapshot up to date
       async for change in drive.sync():
           print(change)

       # Navigate
       root = await drive.get_root()
       children = await drive.get_children(root)

       # Upload and download
       node = await upload_file_from_local(drive, path, root)
       await download_file_to_local(drive, node, dest_dir)

Implementing Services
---------------------

To support a specific cloud provider, implement
:class:`~wcpan.drive.core.types.FileService` and
:class:`~wcpan.drive.core.types.SnapshotService`:

.. code-block:: python

   from contextlib import asynccontextmanager
   from wcpan.drive.core.types import FileService, Node

   class MyFileService(FileService):
       @property
       def api_version(self) -> int:
           return 5

       async def get_root(self) -> Node:
           ...  # fetch root from cloud API

       # implement remaining abstract methods ...

   @asynccontextmanager
   async def create_my_file_service():
       service = MyFileService()
       try:
           yield service
       finally:
           await service.close()

Multiple Sources
----------------

:func:`~wcpan.drive.core.create_multi_drive` accepts a list of
:class:`~wcpan.drive.core.types.SourceConfig` to combine multiple cloud
accounts into a single virtual drive:

.. code-block:: python

   from wcpan.drive.core import create_multi_drive
   from wcpan.drive.core.types import SourceConfig

   async with create_multi_drive(
       sources=[
           SourceConfig(name="account1", file=create_service_a, snapshot=create_snapshot_a),
           SourceConfig(name="account2", file=create_service_b, snapshot=create_snapshot_b),
       ]
   ) as drive:
       ...
