from base import CRUDTestCase, SingleItemTestCase


class BootPoolTestCase(SingleItemTestCase):
    name = 'boot/pool'

    def test_retrieve(self):
        r = super(BootPoolTestCase, self).test_retrieve()
        data = r.json()
        self.assertEqual(data['name'], 'freenas-boot')


class BootEnvironmentTestCase(CRUDTestCase):
    name = 'boot/environment'
