from base import SingleItemBase, Resource, ResourceQueryMixin


class ServiceResource(Resource, ResourceQueryMixin):

    def run_put(self, req, urlparams):
        return req.context['doc']


class Service(SingleItemBase):
    namespace = 'service'
    resource_class = ServiceResource

    def get_retrieve_method_name(self):
        return 'service.query'


def _init(rest):
    rest.register_singleitem(Service)
