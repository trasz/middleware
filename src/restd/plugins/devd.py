from base import ProviderMixin, Resource


class SystemDeviceResource(ProviderMixin, Resource):
    name = 'system/device'
    provider = 'system.device'


def _init(rest):
    rest.register_resource(SystemDeviceResource)
