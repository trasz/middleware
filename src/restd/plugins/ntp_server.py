from base import CRUDBase


class NTPServerResource(CRUDBase):
    namespace = 'ntp_server'


def _init(rest):
    rest.register_crud(NTPServerResource)
