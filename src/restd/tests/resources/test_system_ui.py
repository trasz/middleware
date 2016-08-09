from base import CRUDTestCase, SingleItemTestCase


class SystemUITestCase(SingleItemTestCase):
    name = 'system/ui'

    def get_update_data(self):
        # FIXME: try HTTPS
        return {
            'webui_protocol': ['HTTP'],
            'webui_https_port': 444,
        }
