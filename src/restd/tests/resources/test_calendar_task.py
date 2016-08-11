from base import CRUDTestCase


class CalendarTaskTestCase(CRUDTestCase):
    name = 'calendar_task'

    def get_create_data(self):
        return {
            'name': 'volume.scrub',
            'args': ['tank'],
            'schedule': {
                'second': '0',
                'month': '*',
                'year': '*',
                'hour': '3',
                'coalesce': True,
                'day_of_week': '*',
                'week': '1',
                'minute': '1',
                'timezone': 'UTC',
                'day': '*'
            }
        }

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'name': 'volume.scrub',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id'], {
                'args': ['tank'],
            }
        else:
            self.skipTest('Calendar task not found.')

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'name': 'volume.scrub',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id']
        else:
            self.skipTest('Calendar task not found.')

    def test_050_run(self):
        r = self.client.get(self.name, params={
            'name': 'volume.scrub',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        taskid = data[0]['id']
        r = self.client.post(self.name + '/id/' + taskid + '/run')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_051_command(self):
        r = self.client.post(self.name + '/command', data=[
            'nobody', 'touch /tmp/nobody',
        ])
        self.assertEqual(r.status_code, 201, msg=r.text)
