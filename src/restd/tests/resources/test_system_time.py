from base import CRUDTestCase, SingleItemTestCase


class SystemTimeTestCase(SingleItemTestCase):
    name = 'system/time'

    def get_update_data(self):
        return {
            'timezone': 'America/Sao_Paulo',
        }
