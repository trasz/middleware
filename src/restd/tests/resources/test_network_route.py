from base import CRUDTestCase


class NetworkRouteTestCase(CRUDTestCase):
    name = 'network/route'

    def get_create_data(self):
        return {
            'id': 'testroute',
            'type': 'INET',
            'network': '1.1.0.0',
            'netmask': 16,
            'gateway': '192.168.3.1',
        }

    # FIXME: seems to crash dispatcher?
    # def get_update_ident_data(self):
    #     return 'testroute', {
    #             'gateway': '192.168.3.2',
    #     }

    def get_delete_identifier(self):
        return 'testroute'
