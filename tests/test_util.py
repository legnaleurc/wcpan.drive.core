import unittest

from wcpan.drive.core.util import is_valid_name


class TestUtilities(unittest.TestCase):

    def testIsValidName(self):
        ok = is_valid_name('name')
        self.assertTrue(ok)

        ok = is_valid_name('name/name')
        self.assertFalse(ok)

        ok = is_valid_name('./name')
        self.assertFalse(ok)

        ok = is_valid_name('name\\/name')
        self.assertFalse(ok)

        ok = is_valid_name('name\\name')
        self.assertFalse(ok)
