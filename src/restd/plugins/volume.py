from base import CRUDBase, ItemResource, Resource


class VolumeUpgradeResource(Resource):
    name = 'upgrade'
    post = 'task:volume.upgrade'


class VolumeItemResource(ItemResource):

    def run_get(self, req, kwargs):
        id = kwargs['id']
        return [[['or', [('name', '=', id), ('id', '=', id)]]], {'single': True}], {}


class VolumeCRUD(CRUDBase):
    name = 'volume'
    namespace = 'volume'
    item_class = VolumeItemResource
    item_resources = (
        VolumeUpgradeResource,
    )

    def get_delete_method_name(self):
        return '{0}.destroy'.format(self.namespace)


def _init(rest):
    rest.register_crud(VolumeCRUD)
