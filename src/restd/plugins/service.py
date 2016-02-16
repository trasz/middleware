from base import ServiceBase


class FTPService(ServiceBase):
    namespace = 'ftp'


class SSHService(ServiceBase):
    namespace = 'ssh'


def _init(rest):
    rest.register_service(FTPService)
    rest.register_service(SSHService)
