from base import SingleItemTestCase


class UpdateTestCase(SingleItemTestCase):
    name = 'update'

    def get_update_data(self):
        return {
            'check_auto': False,
        }

    def test_060_check(self):
        r = self.client.post(self.name + '/check')
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()
        self.assertEqual(data, None)

    def test_071_is_update_available(self):
        r = self.client.get(self.name + '/is_update_available')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, bool)

    def test_072_obtain_changelog(self):
        r = self.client.get(self.name + '/obtain_changelog')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data is None:
            return
        self.assertIsInstance(data, (list, str))

    def test_073_get_update_ops(self):
        r = self.client.get(self.name + '/get_update_ops')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data is None:
            return
        self.assertIsInstance(data, list)

    def test_074_update_info(self):
        r = self.client.get(self.name + '/update_info')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data is None:
            return
        self.assertIsInstance(data, dict)

    def test_075_trains(self):
        r = self.client.get(self.name + '/trains')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data is None:
            return
        self.assertIsInstance(data, list)

    def test_076_get_current_train(self):
        r = self.client.get(self.name + '/get_current_train')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, str)
