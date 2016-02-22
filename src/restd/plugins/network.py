from base import CRUDBase, Resource


class InterfaceDownResource(Resource):
    name = 'down'
    post = 'task:network.interface.down'

    def run_post(self, req, kwargs):
        return [kwargs['id']], {}


class InterfaceUpResource(Resource):
    name = 'up'
    post = 'task:network.interface.up'

    def run_post(self, req, kwargs):
        return [kwargs['id']], {}


class InterfaceRenewResource(Resource):
    name = 'up'
    post = 'task:network.interface.renew'

    def run_post(self, req, kwargs):
        return [kwargs['id']], {}


class InterfaceCRUD(CRUDBase):
    name = 'network/interface'
    namespace = 'network.interface'
    item_resources = (
        InterfaceDownResource,
        InterfaceUpResource,
        InterfaceRenewResource,
    )

    def get_update_method_name(self):
        return '{0}.configure'.format(self.namespace)


class RouteCRUD(CRUDBase):
    name = 'network/route'
    namespace = 'network.route'


def _init(rest):
    rest.register_crud(InterfaceCRUD)
    rest.register_crud(RouteCRUD)
