from base import CRUDBase


class TunableResource(CRUDBase):
    namespace = 'tunable'


def _init(rest):
    rest.register_crud(TunableResource)
