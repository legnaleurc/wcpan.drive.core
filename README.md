# wcpan.drive

Asynchronous generic cloud drive library.

This package needs a driver to actually work with a cloud drive.

## Example Usage

```python
from wcpan.drive.core.drive import (
    DriveFactory, download_to_local, upload_from_local,
)


async def simple_demo():
    # Load config and data from default locations.
    factory = DriveFactory()
    factory.load_config()

    async with factory() as drive:
        # It is important to keep cache in sync.
        async for change in drive.sync():
            print(change)

        # Get the root node.
        root_node = await drive.get_root_node()

        # Get a node.
        node = await drive.get_node_by_path('/path/to/drive/file')

        # List children.
        children = await drive.get_children(root_node)

        # Make a folder.
        new_folder = await drive.create_folder(root_node, 'folder_name')

        # Download file.
        await download_to_local(drive, node, '/tmp')

        # Upload file.
        new_file = await upload_from_local(drive, root_node, '/path/to/local/file')

        # Traverse drive.
        async for root, folders, files in drive.walk(root_node):
            print(root, folders, files)


async def config_demo():
    factory = DriveFactory()

    # Read config files from here.
    # The default is $HOME/.config/wcpan/drive.
    # These files are what you want to keep and backup.
    factory.config_path = '/tmp/config'

    # Put data file to here.
    # The default is $HOME/.local/share/wcpan/drive.
    # These files can be safely deleted.
    factory.data_path = '/tmp/data'

    # Setup cache database, will write to data folder.
    factory.database = 'nodes.sqlite'

    # Setup driver class.
    factory.driver = 'some.random.driver.RandomDriver'

    # load config file from config folder
    # this will not overwrite the above given values
    factory.load_config()
```
