from base import CRUDTestCase, SingleItemTestCase


class BootPoolTestCase(SingleItemTestCase):
    name = 'boot/pool'

    def test_020_retrieve(self):
        r = super(BootPoolTestCase, self).test_020_retrieve()
        data = r.json()
        self.assertEqual(data['name'], 'freenas-boot')


class BootEnvironmentTestCase(CRUDTestCase):
    name = 'boot/environment'

    def get_create_data(self):
        return ['newtestenv']

    def get_delete_identifier(self):
        return 'newtestenv'
