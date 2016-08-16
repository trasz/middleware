from base import ProviderMixin, Resource, ResourceQueryMixin


class StatResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'stat'
    provider = 'stat'
    get = 'rpc:stat.query'


def _init(rest):
    rest.register_resource(StatResource)
