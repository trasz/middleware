from base import RESTTestCase


class AlertTestCase(RESTTestCase):
    name = 'alert'

    def test_020_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_021_get_alert_classes(self):
        r = self.client.get(self.name + '/get_alert_classes')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
