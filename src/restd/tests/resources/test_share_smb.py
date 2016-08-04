from base import CRUDTestCase


class ShareSMBTestCase(CRUDTestCase):
    name = 'share'

    def get_create_data(self):
        return {
            'name': 'share_smb_test',
            'description': 'Share SMB Test',
            'target_type': 'DATASET',
            'target_path': 'tank/share_smb_test',
            'type': 'smb',
            'properties': {'type': 'service-smb'},
        }
