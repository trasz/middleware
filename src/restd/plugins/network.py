from base import CRUDBase, ItemResource


class HostCRUD(CRUDBase):
    namespace = 'network.host'


class InterfaceDownResource(ItemResource):
    name = 'down'
    post = 'task:network.interface.down'


class InterfaceUpResource(ItemResource):
    name = 'up'
    post = 'task:network.interface.up'


class InterfaceRenewResource(ItemResource):
    name = 'renew'
    post = 'task:network.interface.renew'


class InterfaceCRUD(CRUDBase):
    namespace = 'network.interface'
    item_resources = (
        InterfaceDownResource,
        InterfaceUpResource,
        InterfaceRenewResource,
    )


class RouteCRUD(CRUDBase):
    namespace = 'network.route'


def _init(rest):
    rest.register_crud(HostCRUD)
    rest.register_crud(InterfaceCRUD)
    rest.register_crud(RouteCRUD)
