from base import Resource, ResourceQueryMixin


class SyslogResource(ResourceQueryMixin, Resource):
    name = 'syslog'
    get = 'rpc:syslog.query'


def _init(rest):
    rest.register_resource(SyslogResource)
