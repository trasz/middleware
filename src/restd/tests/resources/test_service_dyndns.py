from base import SingleItemTestCase


class ServiceDynDNSTestCase(SingleItemTestCase):
    name = 'service/dyndns'

    def get_update_data(self):
        return {
            'provider': 'dyndns@dyndns.org',
            'domains': ['apitest.dyndns.org'],
        }
