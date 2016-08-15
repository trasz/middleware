from base import ProviderMixin, Resource, SingleItemBase


class AlertResource(ProviderMixin, Resource):
    name = 'alert'
    provider = 'alert'
    get = 'rpc:alert.query'


def _init(rest):
    rest.register_resource(AlertResource)
