from base import CRUDBase


class DirectoryCRUD(CRUDBase):
    namespace = 'directory'


def _init(rest):
    rest.register_crud(DirectoryCRUD)
