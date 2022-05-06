import pickle
import unittest

from wcpan.drive.core.util import is_valid_name
from wcpan.drive.core.test import NodeHasher


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


class TestNodeHasher(unittest.TestCase):

    def testIsPicklable(self):
        hasher = NodeHasher()
        binary = pickle.dumps(hasher)
        cloned_hasher = pickle.loads(binary)
        self.assertIsNotNone(cloned_hasher)
