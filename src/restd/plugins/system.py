from base import Resource, SingleItemBase


class SystemAdvancedSingleItem(SingleItemBase):
    namespace = 'system.advanced'


class SystemGeneralSingleItem(SingleItemBase):
    namespace = 'system.general'


class SystemTimeSingleItem(SingleItemBase):
    namespace = 'system.time'


class SystemUISingleItem(SingleItemBase):
    namespace = 'system.ui'


class SystemInfoUnameFullResource(Resource):
    name = 'system/info/uname_full'
    get = 'rpc:system.info.uname_full'


class SystemInfoVersionResource(Resource):
    name = 'system/info/version'
    get = 'rpc:system.info.version'


class SystemInfoLoadAvgResource(Resource):
    name = 'system/info/load_avg'
    get = 'rpc:system.info.load_avg'


class SystemInfoHardwareResource(Resource):
    name = 'system/info/hardware'
    get = 'rpc:system.info.hardware'


def _init(rest):
    rest.register_singleitem(SystemAdvancedSingleItem)
    rest.register_singleitem(SystemGeneralSingleItem)
    rest.register_singleitem(SystemTimeSingleItem)
    rest.register_singleitem(SystemUISingleItem)
    rest.register_resource(SystemInfoUnameFullResource)
    rest.register_resource(SystemInfoVersionResource)
    rest.register_resource(SystemInfoLoadAvgResource)
    rest.register_resource(SystemInfoHardwareResource)
