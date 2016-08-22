from base import CRUDBase, Resource


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


def _init(rest):
    rest.register_crud(DockerContainerCRUD)
