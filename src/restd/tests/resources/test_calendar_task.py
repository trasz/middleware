from base import CRUDTestCase


class CalendarTaskTestCase(CRUDTestCase):
    name = 'calendar_task'

    def get_create_data(self):
        return {
            'id': 'tasktest',
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
        return 'tasktest', {
            'args': ['tank'],
        }

    def get_delete_identifier(self):
        return 'tasktest'

    def test_050_run(self):
        r = self.client.post(self.name + '/id/tasktest/run')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_051_command(self):
        r = self.client.post(self.name + '/command', data=[
            'nobody', 'touch /tmp/nobody',
        ])
        self.assertEqual(r.status_code, 201, msg=r.text)
