from base import ServiceBase


class AFPService(ServiceBase):
    namespace = 'afp'


class DynDNSService(ServiceBase):
    namespace = 'dyndns'


class FTPService(ServiceBase):
    namespace = 'ftp'


class NFSService(ServiceBase):
    namespace = 'nfs'


class SSHService(ServiceBase):
    namespace = 'ssh'


def _init(rest):
    rest.register_service(AFPService)
    rest.register_service(DynDNSService)
    rest.register_service(FTPService)
    rest.register_service(NFSService)
    rest.register_service(SSHService)
