from base import CRUDTestCase


class PeerTestCase(CRUDTestCase):
    name = 'peer'

    def get_create_data(self):
        return {
            'id': 'peertest',
            'name': 'peertest',
            'address': 'peertest',
            'type': 'ssh',
            'credentials': {
                'type': 'ssh',
                'username': 'root',
                'port': 22,
                'password': 'test',
            },
        }

    def get_update_ident_data(self):
        return 'peertest', {
            'address': 'peertest2',
        }

    def get_delete_identifier(self):
        return 'peertest'
