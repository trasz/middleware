from base import CRUDTestCase, SingleItemTestCase


class DocketTestCase(SingleItemTestCase):
    name = 'docker'

    def get_update_data(self):
        return {
            'default_host': None,
        }
