from base import SingleItemBase


class SystemGeneralSingleItem(SingleItemBase):
    namespace = 'system.general'


def _init(rest):
    rest.register_singleitem(SystemGeneralSingleItem)
