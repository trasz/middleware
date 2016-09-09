from base import SingleItemTestCase


class SystemDatasetTestCase(SingleItemTestCase):
    name = 'system_dataset'

    def get_update_data(self):
        return 'freenas-boot'

    def test_041_import(self):
        r = self.client.post('{0}/import'.format(self.name), data=['freenas-boot'])
        self.assertEqual(r.status_code, 201, msg=r.text)
