from base import SingleItemTestCase


class ServiceAFPTestCase(SingleItemTestCase):
    name = 'service/afp'

    def get_update_data(self):
        return {
            'guest_enable': True,
            'connections_limit': 100,
        }
