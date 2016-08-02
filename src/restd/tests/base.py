import os
import unittest

from client import Client


class RESTTestCase(unittest.TestCase):

    def setUp(self):
        self.client = Client(os.environ['URI'], '/api/v2.0/')


class CRUDTestCase(RESTTestCase):

    name = None

    def get_create_data(self):
        raise NotImplementedError('get_create_data needs to be implemented')

    def test_create(self):
        r = self.client.post(self.name, self.get_create_data())
        self.assertEqual(r.status_code, 201)
        data = r.json()
        return r

    def test_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIsInstance(data, list)
        return r


class SingleItemTestCase(RESTTestCase):

    name = None

    def test_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIsInstance(data, dict)
        return r
