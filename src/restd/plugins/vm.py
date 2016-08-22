from base import CRUDBase, Resource


class ExportResource(Resource):
    name = 'export'
    post = 'task:vm.export'


class ImportResource(Resource):
    name = 'import'
    post = 'task:vm.import'


class StartResource(Resource):
    name = 'start'
    post = 'task:vm.start'


class StopResource(Resource):
    name = 'stop'
    post = 'task:vm.stop'


class RebootResource(Resource):
    name = 'reboot'
    post = 'task:vm.reboot'


class VmCRUD(CRUDBase):
    namespace = 'vm'
    item_resources = (
        ExportResource,
        ImportResource,
        StartResource,
        StopResource,
        RebootResource,
    )


class SnapshotPublishResource(Resource):
    name = 'publish'
    post = 'task:vm.snapshot.publish'


class SnapshotRollbackResource(Resource):
    name = 'rollback'
    post = 'task:vm.snapshot.rollback'


class VmSnapshotCRUD(CRUDBase):
    namespace = 'vm.snapshot'
    item_resources = (
        SnapshotPublishResource,
        SnapshotRollbackResource,
    )


class VmTemplateCRUD(CRUDBase):
    namespace = 'vm.template'


def _init(rest):
    rest.register_crud(VmCRUD)
    rest.register_crud(VmSnapshotCRUD)
    rest.register_crud(VmTemplateCRUD)
