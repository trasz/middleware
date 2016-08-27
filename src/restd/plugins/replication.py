from base import CRUDBase, ProviderMixin, Resource, ResourceQueryMixin


class ReplicateDatasetResource(Resource):
    name = 'replicate_dataset'
    post = 'task:replication.replicate_dataset'


class ReplicationResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'replication/link'
    provider = 'replication'
    get = 'rpc:replication.query'
    subresources = (
        ReplicateDatasetResource,
    )


class ReplicationCRUD(CRUDBase):
    namespace = 'replication'

    def get_retrieve_method_name(self):
        return None


def _init(rest):
    rest.register_resource(ReplicationResource)
    rest.register_crud(ReplicationCRUD)
