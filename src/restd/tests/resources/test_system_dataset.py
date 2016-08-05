from base import SingleItemTestCase


class SystemDatasetTestCase(SingleItemTestCase):
    name = 'system_dataset'

    def get_update_data(self):
        return 'freenas-boot'
