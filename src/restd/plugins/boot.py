from base import CRUDBase, SingleItemBase


class BootPoolItem(SingleItemBase):
    namespace = 'boot.pool'

    def get_update_method_name(self):
        # This namespace has no update method
        return None


def _init(rest):
    rest.register_singleitem(BootPoolItem)
