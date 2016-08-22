from base import CRUDBase


class CertificateCRUD(CRUDBase):
    namespace = 'crypto.certificate'


def _init(rest):
    rest.register_crud(CertificateCRUD)
