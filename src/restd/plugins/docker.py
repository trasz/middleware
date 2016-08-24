from base import CRUDBase, ItemResource, ProviderMixin, Resource, ResourceQueryMixin, SingleItemBase


class ContainerStartResource(Resource):
    name = 'start'
    post = 'task:docker.container.start'


class ContainerStopResource(Resource):
    name = 'stop'
    post = 'task:docker.container.stop'


class DockerContainerCRUD(CRUDBase):
    namespace = 'docker.container'
    entity_post = 'atask:docker.container.create'
    item_resources = (
        ContainerStartResource,
        ContainerStopResource,
    )

    def get_update_method_name(self):
        return None


class ImagePullResource(ItemResource):
    name = 'pull'
    post = 'atask:docker.image.pull'


class DockerImageCRUD(CRUDBase):
    namespace = 'docker.image'
    item_resources = (
        ImagePullResource,
    )

    def get_create_method_name(self):
        return None

    def get_update_method_name(self):
        return None


class DockerSingleItem(SingleItemBase):
    namespace = 'docker'


class DockerHostResource(ProviderMixin, ResourceQueryMixin, Resource):
    name = 'docker/host'
    provider = 'docker.host'
    get = 'rpc:docker.host.query'


def _init(rest):
    rest.register_singleitem(DockerSingleItem)
    rest.register_crud(DockerContainerCRUD)
    rest.register_crud(DockerImageCRUD)
    rest.register_resource(DockerHostResource)
