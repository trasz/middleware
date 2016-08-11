from base import CRUDBase


class DirectoryResource(CRUDBase):
    namespace = 'directory'


def _init(rest):
    rest.register_crud(DirectoryResource)
