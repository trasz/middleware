from base import CRUDBase


class ShareResource(CRUDBase):
    namespace = 'share'


def _init(rest):
    rest.register_crud(ShareResource)
