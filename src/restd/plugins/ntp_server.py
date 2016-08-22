from base import CRUDBase


class NTPServerCRUD(CRUDBase):
    namespace = 'ntp_server'


def _init(rest):
    rest.register_crud(NTPServerCRUD)
