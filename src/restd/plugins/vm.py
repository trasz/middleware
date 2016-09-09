from base import CRUDBase, ItemResource, Resource


class ExportResource(ItemResource):
    name = 'export'
    post = 'task:vm.export'


class ImportResource(Resource):
    name = 'import'
    post = 'task:vm.import'


class StartResource(ItemResource):
    name = 'start'
    post = 'task:vm.start'


class StopResource(ItemResource):
    name = 'stop'
    post = 'task:vm.stop'


class RebootResource(ItemResource):
    name = 'reboot'
    post = 'task:vm.reboot'


class VmCRUD(CRUDBase):
    namespace = 'vm'
    entity_post = 'atask:vm.create'
    item_resources = (
        ExportResource,
        StartResource,
        StopResource,
        RebootResource,
    )
    entity_resources = (
        ImportResource,
    )


class SnapshotPublishResource(ItemResource):
    name = 'publish'
    post = 'task:vm.snapshot.publish'


class SnapshotRollbackResource(ItemResource):
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

    def get_create_method_name(self):
        return None

    def get_update_method_name(self):
        return None


def _init(rest):
    rest.register_crud(VmCRUD)
    rest.register_crud(VmSnapshotCRUD)
    rest.register_crud(VmTemplateCRUD)
