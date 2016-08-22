from base import SingleItemBase, Resource


class ImportResource(Resource):
    name = 'import'
    post = 'task:system_dataset.import'


class SystemDatasetSingleItem(SingleItemBase):
    namespace = 'system_dataset'

    subresources = (
        ImportResource,
    )

    def get_retrieve_method_name(self):
        return 'system_dataset.status'

    def get_update_method_name(self):
        return 'system_dataset.migrate'


def _init(rest):
    rest.register_singleitem(SystemDatasetSingleItem)
