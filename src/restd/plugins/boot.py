from base import CRUDBase, EntityResource, SingleItemBase


class BootPoolItem(SingleItemBase):
    namespace = 'boot.pool'

    def get_update_method_name(self):
        # This namespace has no update method
        return None


class BootEnvironmentEntity(EntityResource):

    def run_post(self, req, kwargs):
        return req.context['doc']


class BootEnvironmentResource(CRUDBase):
    namespace = 'boot.environment'
    entity_class = BootEnvironmentEntity

    def get_create_method_name(self):
        return 'boot.environment.clone'


def _init(rest):
    rest.register_singleitem(BootPoolItem)
    rest.register_crud(BootEnvironmentResource)
