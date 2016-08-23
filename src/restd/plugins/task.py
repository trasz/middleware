from base import Resource, ResourceQueryMixin, SingleItemBase


class QueryResource(Resource, ResourceQueryMixin):
    name = 'task'
    get = 'rpc:task.query'


def _init(rest):
    rest.register_resource(QueryResource)
