from base import ProviderMixin, Resource, ResourceQueryMixin


class SessionResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'session'
    provider = 'session'
    get = 'rpc:session.query'


def _init(rest):
    rest.register_resource(SessionResource)
