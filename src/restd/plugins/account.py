from base import CRUDBase


class UserCRUD(CRUDBase):
    namespace = 'user'


def _init(rest):
    rest.register_crud(UserCRUD)
