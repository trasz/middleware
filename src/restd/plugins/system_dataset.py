from base import SingleItemBase


class SystemDataset(SingleItemBase):
    namespace = 'system_dataset'

    def get_retrieve_method_name(self):
        return 'system_dataset.status'

    def get_update_method_name(self):
        return 'system_dataset.migrate'


def _init(rest):
    rest.register_singleitem(SystemDataset)
