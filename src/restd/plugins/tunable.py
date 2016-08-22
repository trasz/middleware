from base import CRUDBase


class TunableCRUD(CRUDBase):
    namespace = 'tunable'


def _init(rest):
    rest.register_crud(TunableCRUD)
