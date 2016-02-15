import falcon

from freenas.dispatcher.rpc import RpcException

from swagger import normalize_schema


class Task(object):

    name = None

    def __init__(self, resource, dispatcher, name=None):
        if name is not None:
            self.name = name
        self.resouce = resource
        self.dispatcher = dispatcher

    def run(self, req, kwargs):
        try:
            result = self.dispatcher.call_task_sync(self.name, req.context['doc'])
        except RpcException as e:
            raise falcon.HTTPBadRequest(e.message, str(e))
        if result['state'] != 'FINISHED':
            if result['error']:
                title = result['error']['type']
                message = result['error']['message']
            else:
                title = 'UnknownError'
                message = 'Failed to create, check task #{0}'.format(result['id'])
            raise falcon.HTTPBadRequest(title, message)
        return result


class RPC(object):

    name = None

    def __init__(self, resource, dispatcher, method, name=None):
        if name is not None:
            self.name = name
        self.resource = resource
        self.dispatcher = dispatcher
        self.method = method

    def run(self, req, kwargs):
        run_args = getattr(self.resource, 'run_{0}'.format(self.method), None)
        args = []
        if run_args:
            args, kwargs = run_args(req, kwargs)
        try:
            result = self.dispatcher.call_sync(self.name, args, kwargs)
        except RpcException as e:
            raise falcon.HTTPBadRequest(e.message, str(e))
        return result


class Resource(object):

    name = None
    parent = None

    get = None
    post = None
    put = None
    delete = None

    def __init__(self, rest, parent=None):
        self.rest = rest
        self.children = []

        if parent is not None:
            parent.add_child(self)

        self.rest.api.add_route(self.get_uri(), self)

    def __getattr__(self, attr):
        if attr in ('on_get', 'on_post', 'on_delete', 'on_put'):
            do = object.__getattribute__(self, 'do')
            method = attr.split('_')[-1]

            if object.__getattribute__(self, method) is None:
                return None

            def on_method(req, resp, **kwargs):
                return do(method, req, resp, **kwargs)

            return on_method
        return object.__getattribute__(self, attr)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def get_uri(self):
        if self.parent is None:
            return '/{0}'.format(self.name)
        return '{0}/{1}'.format(self.parent.get_uri(), self.name)

    def _get_type_name(self, method_op):
        if ':' not in method_op:
            raise falcon.HTTPError(
                falcon.HTTP_500, 'Internal Error', 'No valid operation provided'
            )
        type_, name = method_op.split(':', 1)
        assert type_ in ('task', 'rpc')
        return type_, name

    def do(self, method, req, resp, **kwargs):
        method_op = getattr(self, method)
        type_, name = self._get_type_name(method_op)

        if type_ == 'task':
            t = Task(self, req.context['client'], name=name)
            return t.run(req, kwargs)
        else:
            r = RPC(self, req.context['client'], method, name=name)
            req.context['result'] = r.run(req, kwargs)


    def doc(self):
        rv = {}
        for i in ('get', 'post', 'put', 'delete'):
            method_op = getattr(self, i, None)
            if method_op is None:
                continue
            type_, name = self._get_type_name(method_op)
            if type_ == 'task':
                op = self.rest._tasks[name]
            else:
                op = self.rest._rpcs[name]

            rv[i] = {
                'responses': {
                'description': op.get('description'),
                    '200': {
                        'description': 'entries to be returned',
                        'schema': normalize_schema(op.get('result-schema')),
                    }
                },
            }

            if i in ('post', 'put'):
                rv[i]['parameters'] = [
                    {
                        'name': 'data',
                        'in': 'body',
                        'required': True,
                        'schema': normalize_schema(op.get('schema')),
                    },
                ],
        return rv


class ItemResource(Resource):

    name = '{id}'

    def run_get(self, req, kwargs):
        return [('id', '=', int(kwargs['id']))], {'single': True}


class CRUDBase(object):

    entity_class = Resource
    item_class = ItemResource
    namespace = None

    def __init__(self, rest, dispatcher):

        class MyEntity(self.entity_class):
            name = self.namespace
            get = 'rpc:{0}'.format(self.get_retrieve_method_name())
            post = 'task:{0}'.format(self.get_create_method_name())

        class MyItem(self.item_class):
            get = 'rpc:{0}'.format(self.get_retrieve_method_name())
            put = 'task:{0}'.format(self.get_update_method_name())
            delete = 'task:{0}'.format(self.get_delete_method_name())

        self.entity = MyEntity(rest)
        MyItem(rest, parent=self.entity)

    def get_create_method_name(self):
        return '{0}.create'.format(self.namespace)

    def get_retrieve_method_name(self):
        return '{0}.query'.format(self.namespace)

    def get_update_method_name(self):
        return '{0}.update'.format(self.namespace)

    def get_delete_method_name(self):
        return '{0}.delete'.format(self.namespace)
