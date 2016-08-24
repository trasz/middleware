from base import CRUDBase, ProviderMixin, Resource, ResourceQueryMixin, SingleItemBase


class ContainerStartResource(Resource):
    name = 'start'
    post = 'task:docker.container.start'


class ContainerStopResource(Resource):
    name = 'stop'
    post = 'task:docker.container.stop'


class DockerContainerCRUD(CRUDBase):
    namespace = 'docker.container'
    item_resources = (
        ContainerStartResource,
        ContainerStopResource,
    )

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
    rest.register_resource(DockerHostResource)
