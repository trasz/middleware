from base import SingleItemBase


class SystemAdvancedSingleItem(SingleItemBase):
    namespace = 'system.advanced'


class SystemGeneralSingleItem(SingleItemBase):
    namespace = 'system.general'


class SystemTimeSingleItem(SingleItemBase):
    namespace = 'system.time'


class SystemUISingleItem(SingleItemBase):
    namespace = 'system.ui'


def _init(rest):
    rest.register_singleitem(SystemAdvancedSingleItem)
    rest.register_singleitem(SystemGeneralSingleItem)
    rest.register_singleitem(SystemTimeSingleItem)
    rest.register_singleitem(SystemUISingleItem)
