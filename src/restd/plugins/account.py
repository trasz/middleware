from base import CRUDBase


class GroupCRUD(CRUDBase):
    namespace = 'group'


class UserCRUD(CRUDBase):
    namespace = 'user'


def _init(rest):
    rest.register_crud(GroupCRUD)
    rest.register_crud(UserCRUD)
