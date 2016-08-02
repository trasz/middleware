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


class SSHDService(SingleItemBase):
    namespace = 'service.sshd'


class TFTPDService(SingleItemBase):
    namespace = 'service.tftpd'


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
    rest.register_singleitem(SSHDService)
    rest.register_singleitem(TFTPDService)
    rest.register_singleitem(UPSService)
    rest.register_singleitem(WebDAVService)
