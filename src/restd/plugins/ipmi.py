from base import Resource, ResourceQueryMixin, SingleItemBase


class IPMIQueryResource(Resource, ResourceQueryMixin):
    pass


class IPMISingleItem(SingleItemBase):
    namespace = 'ipmi'
    resource_class = IPMIQueryResource

    def get_retrieve_method_name(self):
        return 'ipmi.query'


def _init(rest):
    rest.register_singleitem(IPMISingleItem)
