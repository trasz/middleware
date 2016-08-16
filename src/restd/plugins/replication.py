from base import CRUDBase, ProviderMixin, Resource, ResourceQueryMixin


class CalculateDeltaResource(Resource):
    name = 'calculate_delta'
    post = 'task:replication.calculate_delta'


class ReplicateDatasetResource(Resource):
    name = 'replicate_dataset'
    post = 'task:replication.replicate_dataset'


class ReplicationLinkResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'replication/link'
    provider = 'replication.link'
    get = 'rpc:replication.link.query'
    subresources = (
        CalculateDeltaResource,
        ReplicateDatasetResource,
    )


class ReplicationCRUD(CRUDBase):
    namespace = 'replication'

    def get_retrieve_method_name(self):
        return None


def _init(rest):
    rest.register_resource(ReplicationLinkResource)
    rest.register_crud(ReplicationCRUD)
