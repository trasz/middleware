from base import CRUDBase, ItemResource


class QueryResource(ItemResource):
    name = 'query'
    post = 'task:backup.query'


class RestoreResource(ItemResource):
    name = 'restore'
    post = 'task:backup.restore'


class SyncResource(ItemResource):
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
