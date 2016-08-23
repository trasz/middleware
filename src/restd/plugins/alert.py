from base import CRUDBase, ProviderMixin, Resource


class AlertSendResource(Resource):
    name = 'send'
    post = 'task:alert.send'


class AlertResource(ProviderMixin, Resource):
    name = 'alert'
    provider = 'alert'
    get = 'rpc:alert.query'
    subresources = (
        AlertSendResource,
    )


class AlertFilterCRUD(CRUDBase):
    name = 'alert/filter'
    namespace = 'alert.filter'


def _init(rest):
    rest.register_resource(AlertResource)
    rest.register_crud(AlertFilterCRUD)
