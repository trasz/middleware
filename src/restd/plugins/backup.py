from base import CRUDBase, Resource


class QueryResource(Resource):
    name = 'query'
    post = 'task:backup.query'


class RestoreResource(Resource):
    name = 'restore'
    post = 'task:backup.restore'


class SyncResource(Resource):
    name = 'sync'
    post = 'task:backup.sync'


class BackupCRUD(CRUDBase):
    namespace = 'backup'
    item_resources = (
        QueryResource,
        RestoreResource,
        SyncResource,
    )


def _init(rest):
    rest.register_crud(BackupCRUD)
