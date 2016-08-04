from base import SingleItemTestCase


class ServiceFTPTestCase(SingleItemTestCase):
    name = 'service/ftp'

    def get_update_data(self):
        return {
            'reverse_dns': True,
            'root_login': True,
            'ident': True,
            'filemask': {
                'group': {
                    'read': True,
                    'execute': True,
                    'write': True
                },
                'others': {
                    'read': True,
                    'execute': True,
                    'write': True
                },
                'user': {
                    'read': False,
                    'execute': False,
                    'write': False
                }
            },
            'dirmask': {
                'group': {
                    'read': True,
                    'execute': True,
                    'write': True
                },
                'others': {
                    'read': True,
                    'execute': True,
                    'write': True
                },
                'user': {
                    'read': False,
                    'execute': False,
                    'write': False
                }
            },
        }
