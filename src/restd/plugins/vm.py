from base import CRUDBase, Resource


class ExportResource(Resource):
    name = 'export'
    post = 'task:vm.export'


class StartResource(Resource):
    name = 'start'
    post = 'task:vm.start'


class StopResource(Resource):
    name = 'start'
    post = 'task:vm.start'


class RebootResource(Resource):
    name = 'start'
    post = 'task:vm.start'


class VmCRUD(CRUDBase):
    namespace = 'vm'
    item_resources = (
        ExportResource,
        StartResource,
        StopResource,
        RebootResource,
    )


def _init(rest):
    rest.register_crud(VmCRUD)
