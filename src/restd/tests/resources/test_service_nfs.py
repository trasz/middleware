from base import SingleItemTestCase


class ServiceNFSTestCase(SingleItemTestCase):
    name = 'service/nfs'

    def get_update_data(self):
        return {
            'v4': True,
            'servers': 8,
        }
