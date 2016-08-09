from base import CRUDBase


class ShareResource(CRUDBase):
    namespace = 'share'


class ShareISCSIAuthResource(CRUDBase):
    name = 'share_iscsi/auth'  # FIXME: custom name until we figure out conflicts
    namespace = 'share.iscsi.auth'


class ShareISCSIPortalResource(CRUDBase):
    name = 'share_iscsi/portal'  # FIXME: custom name until we figure out conflicts
    namespace = 'share.iscsi.portal'


class ShareISCSITargetResource(CRUDBase):
    name = 'share_iscsi/target'  # FIXME: custom name until we figure out conflicts
    namespace = 'share.iscsi.target'


def _init(rest):
    rest.register_crud(ShareResource)
    rest.register_crud(ShareISCSIAuthResource)
    rest.register_crud(ShareISCSIPortalResource)
    rest.register_crud(ShareISCSITargetResource)
