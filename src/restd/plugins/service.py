from base import SingleItemBase


class AFPService(SingleItemBase):
    namespace = 'service.afp'


class DynDNSService(SingleItemBase):
    namespace = 'service.dyndns'


class FTPService(SingleItemBase):
    namespace = 'service.ftp'


class NFSService(SingleItemBase):
    namespace = 'service.nfs'


class SMARTDService(SingleItemBase):
    namespace = 'service.smb'


class SMBService(SingleItemBase):
    namespace = 'service.smb'


class SNMPService(SingleItemBase):
    namespace = 'service.snmp'


class SSHService(SingleItemBase):
    namespace = 'service.ssh'


class TFTPService(SingleItemBase):
    namespace = 'service.tftp'


class UPSService(SingleItemBase):
    namespace = 'service.ups'


class WebDAVService(SingleItemBase):
    namespace = 'service.webdav'


def _init(rest):
    rest.register_singleitem(AFPService)
    rest.register_singleitem(DynDNSService)
    rest.register_singleitem(FTPService)
    rest.register_singleitem(NFSService)
    rest.register_singleitem(SMARTDService)
    rest.register_singleitem(SMBService)
    rest.register_singleitem(SNMPService)
    rest.register_singleitem(SSHService)
    rest.register_singleitem(TFTPService)
    rest.register_singleitem(UPSService)
    rest.register_singleitem(WebDAVService)
