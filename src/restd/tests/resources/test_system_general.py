from base import CRUDTestCase, SingleItemTestCase


class SystemGeneralTestCase(SingleItemTestCase):
    name = 'system/general'

    def get_update_data(self):
        return {
            'timezone': 'America/Sao_Paulo',
        }
