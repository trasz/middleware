from base import CRUDBase, ProviderMixin, Resource


class AlertResource(ProviderMixin, Resource):
    name = 'alert'
    provider = 'alert'
    get = 'rpc:alert.query'


class AlertFilterCRUD(CRUDBase):
    name = 'alert/filter'
    namespace = 'alert.filter'


def _init(rest):
    rest.register_resource(AlertResource)
    rest.register_crud(AlertFilterCRUD)
