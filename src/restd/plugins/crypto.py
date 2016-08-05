from base import CRUDBase


class CryptoResource(CRUDBase):
    namespace = 'crypto.certificate'


def _init(rest):
    rest.register_crud(CryptoResource)
