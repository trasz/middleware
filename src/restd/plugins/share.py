from base import CRUDBase


class ShareCRUD(CRUDBase):
    namespace = 'share'


class ShareISCSIAuthCRUD(CRUDBase):
    name = 'share/iscsi/auth'
    namespace = 'share.iscsi.auth'


class ShareISCSIPortalCRUD(CRUDBase):
    name = 'share/iscsi/portal'
    namespace = 'share.iscsi.portal'


class ShareISCSITargetCRUD(CRUDBase):
    name = 'share/iscsi/target'
    namespace = 'share.iscsi.target'


def _init(rest):
    rest.register_crud(ShareCRUD)
    rest.register_crud(ShareISCSIAuthCRUD)
    rest.register_crud(ShareISCSIPortalCRUD)
    rest.register_crud(ShareISCSITargetCRUD)
