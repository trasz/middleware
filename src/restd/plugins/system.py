from base import SingleItemBase


class SystemAdvancedSingleItem(SingleItemBase):
    namespace = 'system.advanced'


class SystemGeneralSingleItem(SingleItemBase):
    namespace = 'system.general'


class SystemUISingleItem(SingleItemBase):
    namespace = 'system.ui'


def _init(rest):
    rest.register_singleitem(SystemAdvancedSingleItem)
    rest.register_singleitem(SystemGeneralSingleItem)
    rest.register_singleitem(SystemUISingleItem)
