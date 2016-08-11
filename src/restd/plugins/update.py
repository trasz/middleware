from base import ProviderMixin, Resource, SingleItemBase


class UpdateSingleItem(SingleItemBase):
    namespace = 'update'


def _init(rest):
    rest.register_singleitem(UpdateSingleItem)
