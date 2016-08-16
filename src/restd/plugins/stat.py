from base import ProviderMixin, Resource, ResourceQueryMixin


class StatResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'stat'
    provider = 'stat'
    get = 'rpc:stat.query'


class StatCpuResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'stat/cpu'
    provider = 'stat.cpu'
    get = 'rpc:stat.cpu.query'


class StatDiskResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'stat/disk'
    provider = 'stat.disk'
    get = 'rpc:stat.disk.query'


class StatSystemResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'stat/system'
    provider = 'stat.system'
    get = 'rpc:stat.system.query'


class StatNetworkResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'stat/network'
    provider = 'stat.network'
    get = 'rpc:stat.network.query'


def _init(rest):
    rest.register_resource(StatResource)
    rest.register_resource(StatCpuResource)
    rest.register_resource(StatDiskResource)
    rest.register_resource(StatSystemResource)
    rest.register_resource(StatNetworkResource)
