from base import CRUDBase


class DiskResource(CRUDBase):
    namespace = 'disk'

    def get_create_method_name(self):
        return None


def _init(rest):
    rest.register_crud(DiskResource)
