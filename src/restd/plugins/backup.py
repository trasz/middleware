from base import CRUDBase


class BackupCRUD(CRUDBase):
    namespace = 'backup'


def _init(rest):
    rest.register_crud(BackupCRUD)
