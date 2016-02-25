from base import CRUDBase, ItemResource, Resource


class DatasetCRUD(CRUDBase):
    name = 'dataset'
    namespace = 'volume.dataset'


class SnapshotCRUD(CRUDBase):
    name = 'snapshot'
    namespace = 'volume.snapshot'

    def get_update_method_name(self):
        return None


class VolumeUpgradeResource(Resource):
    name = 'upgrade'
    post = 'task:volume.upgrade'


class VolumeItemResource(ItemResource):

    def run_get(self, req, kwargs):
        id = kwargs['id']
        return [[['or', [('name', '=', id), ('id', '=', id)]]], {'single': True}], {}


class VolumeCRUD(CRUDBase):
    namespace = 'volume'
    item_class = VolumeItemResource
    item_resources = (
        VolumeUpgradeResource,
    )

    def get_delete_method_name(self):
        return '{0}.destroy'.format(self.namespace)


def _init(rest):
    rest.register_crud(DatasetCRUD)
    rest.register_crud(SnapshotCRUD)
    rest.register_crud(VolumeCRUD)
