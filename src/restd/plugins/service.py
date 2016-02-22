from base import ServiceBase


class AFPService(ServiceBase):
    namespace = 'afp'


class DynDNSService(ServiceBase):
    namespace = 'dyndns'


class FTPService(ServiceBase):
    namespace = 'ftp'


class NFSService(ServiceBase):
    namespace = 'nfs'


class SMARTDService(ServiceBase):
    namespace = 'smb'


class SMBService(ServiceBase):
    namespace = 'smb'


class SNMPService(ServiceBase):
    namespace = 'snmp'


class SSHService(ServiceBase):
    namespace = 'ssh'


class TFTPService(ServiceBase):
    namespace = 'tftp'


class UPSService(ServiceBase):
    namespace = 'ups'


class WebDAVService(ServiceBase):
    namespace = 'webdav'


def _init(rest):
    rest.register_service(AFPService)
    rest.register_service(DynDNSService)
    rest.register_service(FTPService)
    rest.register_service(NFSService)
    rest.register_service(SMARTDService)
    rest.register_service(SMBService)
    rest.register_service(SNMPService)
    rest.register_service(SSHService)
    rest.register_service(TFTPService)
    rest.register_service(UPSService)
    rest.register_service(WebDAVService)
