from base import ProviderMixin, Resource, SingleItemBase


class SystemDeviceResource(ProviderMixin, Resource):
    name = 'system/device'
    provider = 'system.device'


def _init(rest):
    rest.register_resource(SystemDeviceResource)
