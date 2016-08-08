from base import CRUDTestCase


class VolumeTestCase(CRUDTestCase):
    name = 'volume'

    def get_create_data(self):
        return {
            'id': 'tank',
            'type': 'zfs',
            'topology': {
                'data': [{
                    'type': 'disk',
                    'path': '/dev/ada1',
                }]
            },
        }

    def get_update_ident_data(self):
        return 'tank', {
            'id': 'ztank',
        }

    def get_delete_identifier(self):
        return 'ztank'
