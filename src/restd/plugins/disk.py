from base import CRUDBase, Resource


class DiskIsOnlineResource(Resource):
    name = 'is_online'
    post = 'rpc:disk.is_online'


class DiskPartitionToDiskResource(Resource):
    name = 'partition_to_disk'
    post = 'rpc:disk.partition_to_disk'


class DiskCRUD(CRUDBase):
    namespace = 'disk'
    entity_resources = (
        DiskIsOnlineResource,
        DiskPartitionToDiskResource
    )

    def get_create_method_name(self):
        return None


def _init(rest):
    rest.register_crud(DiskCRUD)
