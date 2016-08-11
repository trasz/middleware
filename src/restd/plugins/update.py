from base import Resource, ProviderMixin


class UpdateResource(ProviderMixin, Resource):
    name = 'update'
    provider = 'update'


def _init(rest):
    rest.register_resource(UpdateResource)
