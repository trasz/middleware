from base import CRUDTestCase, SingleItemTestCase


class SystemAdvancedTestCase(SingleItemTestCase):
    name = 'system/advanced'

    def get_update_data(self):
        return {
            'console_screensaver': True,
            'boot_scrub_internal': 70,
        }
