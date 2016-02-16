from base import ServiceBase


class SSHService(ServiceBase):
    namespace = 'ssh'


def _init(rest):
    rest.register_service(SSHService)
    SSHService(rest, rest.dispatcher)
