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
        self.assertEqual(r.status_code, 200)
        data = r.json()
        if data:
            return data[0]['id'], {
                'args': ['tank2'],
            }
        else:
            self.skipTest('Calendar task not found.')

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'name': 'volume.scrub',
        })
        self.assertEqual(r.status_code, 200)
        data = r.json()
        if data:
            return data[0]['id']
        else:
            self.skipTest('Calendar task not found.')
