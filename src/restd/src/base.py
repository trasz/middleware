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
            result = self.dispatcher.call_sync(self.name, *args, **kwargs)
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
            t.run(req, kwargs)
        else:
            r = RPC(self, req.context['client'], method, name=name)
            req.context['result'] = r.run(req, kwargs)

        if method == 'post':
            resp.status = falcon.HTTP_201
        elif method == 'delete':
            resp.status = falcon.HTTP_204

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

            code_map = {
                'post': '201',
                'get': '200',
                'delete': '204',
                'put': '200',
            }

            rv[i] = {
                'description': op.get('description'),
                'responses': {
                    code_map[i]: {
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
                ]
        return rv


class EntityResource(Resource):

    def run_get(self, req, kwargs):
        args = []
        for key, val in req.params.items():
            if '__' in key:
                field, op = key.split('__', 1)
            else:
                field, op = key, '='

            if key in ('sort', 'limit', 'offset'):
                kwargs[key] = val
                continue

            op_map = {
                'eq': '=',
                'neq': '!=',
                'gt': '>',
                'lt': '<',
                'gte': '>=',
                'lte': '<=',
                'regex': '~',
            }

            op = op_map.get(op, op)

            if val.isdigit():
                val = int(val)
            elif val.lower() == 'true':
                val = True
            elif val.lower() == 'false':
                val = False
            args.append((field, op, val))

        return [args, kwargs], {}

    def do(self, method, req, resp, *args, **kwargs):
        rv = super(EntityResource, self).do(method, req, resp, *args, **kwargs)
        if method == 'post':
            kwargs['single'] = True
            rv = self.do('get', req, resp, *args, **kwargs)
            resp.status = falcon.HTTP_201
        return rv


class ItemResource(Resource):

    name = '{id}'

    def run_get(self, req, kwargs):
        return [('id', '=', int(kwargs['id']))], {'single': True}

    def doc(self):
        rv = super(ItemResource, self).doc()
        for i in ('get', 'post', 'put', 'delete'):
            method_op = getattr(self, i, None)
            if method_op is None:
                continue

            if 'parameters' not in rv[i]:
                rv[i]['parameters'] = []

            rv[i]['parameters'] = [
                {
                    'name': 'id',
                    'in': 'path',
                    'required': True,
                    'schema': {'type': 'integer'},
                },
            ]
        return rv


class ProviderMixin:

    provider = None

    def __init__(self, rest, parent=None):
        super(ProviderMixin, self).__init__(rest, parent)
        provider_name = self.get_provider_name()
        if provider_name is None:
            return

        methods = rest._services.get(self.get_provider_name())
        if methods is None:
            return

        for method in methods:
            # Skip private RPC methods
            if method.get('private') is True:
                continue
            type('{0}Resource'.format(method['name']), (Resource, ), {
                'name': method['name'],
                'get': 'rpc:{0}.{1}'.format(provider_name, method['name']),
            })(rest, parent=self)

    def get_provider_name(self):
        if self.provider:
            return self.provider
        if self.get is not None and self.get.startswith('rpc:'):
            return self.get[4:].rsplit('.', 1)[0]


class ServiceResource(Resource):

    def get_uri(self):
        return '/service/{0}'.format(self.name)


class ServiceBase(object):

    def __init__(self, rest, dispatcher):

        type('{0}ServiceResource'.format(self.__class__.__name__), (ProviderMixin, ServiceResource, ), {
            'name': self.namespace,
            'get': 'rpc:{0}'.format(self.get_retrieve_method_name()),
            'put': 'task:{0}'.format(self.get_update_method_name()),
        })(rest)

    def get_retrieve_method_name(self):
        return 'service.{0}.get_config'.format(self.namespace)

    def get_update_method_name(self):
        return 'service.{0}.configure'.format(self.namespace)


class CRUDBase(object):

    entity_class = EntityResource
    item_class = ItemResource
    namespace = None

    def __init__(self, rest, dispatcher):

        self.entity = type('{0}EntityResource'.format(self.__class__.__name__), (self.entity_class, ), {
            'name': self.namespace,
            'get': 'rpc:{0}'.format(self.get_retrieve_method_name()),
            'post': 'task:{0}'.format(self.get_create_method_name()),
        })(rest)
        type('{0}ItemResource'.format(self.__class__.__name__), (self.item_class, ), {
            'get': 'rpc:{0}'.format(self.get_retrieve_method_name()),
            'put': 'task:{0}'.format(self.get_update_method_name()),
            'delete': 'task:{0}'.format(self.get_delete_method_name()),
        })(rest, parent=self.entity)

    def get_create_method_name(self):
        return '{0}.create'.format(self.namespace)

    def get_retrieve_method_name(self):
        return '{0}.query'.format(self.namespace)

    def get_update_method_name(self):
        return '{0}.update'.format(self.namespace)

    def get_delete_method_name(self):
        return '{0}.delete'.format(self.namespace)
