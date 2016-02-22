from base import CRUDBase, Resource


class VolumeUpgradeResource(Resource):
    name = 'upgrade'
    post = 'task:volume.upgrade'

    def run_post(self, req, kwargs):
        return [kwargs['id']], {}


class VolumeCRUD(CRUDBase):
    name = 'volume'
    namespace = 'volume'
    item_resources = (
        VolumeUpgradeResource,
    )

    def get_delete_method_name(self):
        return '{0}.destroy'.format(self.namespace)


def _init(rest):
    rest.register_crud(VolumeCRUD)
