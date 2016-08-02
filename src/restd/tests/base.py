import os
import unittest

from client import Client


class RESTTestCase(unittest.TestCase):

    def setUp(self):
        self.client = Client(os.environ['URI'], '/api/v2.0/')


class CRUDTestCase(RESTTestCase):

    name = None

    def test_list(self):
        r = self.client.get(self.name)
        self.assertTrue(isinstance(r, list))
