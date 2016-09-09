from base import SingleItemBase, Resource, ResourceQueryMixin


class ServiceResource(Resource, ResourceQueryMixin):
    pass


class ServiceSingleItem(SingleItemBase):
    namespace = 'service'
    resource_class = ServiceResource

    def get_retrieve_method_name(self):
        return 'service.query'


def _init(rest):
    rest.register_singleitem(ServiceSingleItem)
