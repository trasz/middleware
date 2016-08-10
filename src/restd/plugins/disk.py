from base import CRUDBase, Resource


class DiskIsOnlineResource(Resource):
    name = 'is_online'
    post = 'rpc:disk.is_online'


class DiskResource(CRUDBase):
    namespace = 'disk'
    entity_resources = (
        DiskIsOnlineResource,
    )

    def get_create_method_name(self):
        return None


def _init(rest):
    rest.register_crud(DiskResource)
