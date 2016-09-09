from base import CRUDBase, ProviderMixin, Resource, ResourceQueryMixin


class ReplicateDatasetResource(Resource):
    name = 'replicate_dataset'
    post = 'task:replication.replicate_dataset'


class ReplicationCRUD(CRUDBase):
    namespace = 'replication'

    entity_resources = (
        ReplicateDatasetResource,
    )


def _init(rest):
    rest.register_crud(ReplicationCRUD)
