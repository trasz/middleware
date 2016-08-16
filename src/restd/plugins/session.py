from base import CRUDBase, ProviderMixin, Resource


class SessionResource(ProviderMixin, Resource):
    name = 'session'
    provider = 'session'
    get = 'rpc:session.query'


def _init(rest):
    rest.register_resource(SessionResource)
