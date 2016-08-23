import falcon
import logging
import pprint

from freenas.dispatcher.rpc import RpcException

from swagger import normalize_schema

log = logging.getLogger('restd.base')


class Task(object):

    name = None

    def __init__(self, resource, dispatcher, method, name=None):
        if name is not None:
            self.name = name
        self.resource = resource
        self.dispatcher = dispatcher
        self.method = method

    def run(self, req, urlparams, get_args=None):
        if get_args is None:
            get_args = getattr(self.resource, 'run_{0}'.format(self.method), None)
        if get_args:
            args = get_args(req, urlparams)
        else:
            if 'doc' in req.context:
                args = req.context['doc']
            else:
                args = []
        try:
            log.debug('Calling task {0} with args {1}'.format(self.name, args))
            result = self.dispatcher.call_task_sync(self.name, *args)
        except RpcException as e:
            raise falcon.HTTPBadRequest(e.message, str(e))
        log.debug('Task {0} finished: {1}'.format(self.name, pprint.pformat(result)))
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

    def run(self, req, urlparams, get_args=None):
        if get_args is None:
            get_args = getattr(self.resource, 'run_{0}'.format(self.method), None)
        if get_args:
            args, urlparams = get_args(req, urlparams)
        else:
            if 'doc' in req.context:
                args = req.context['doc']
            else:
                args = []
        log.debug('Calling RPC {0} with args {1} {2}'.format(self.name, args, urlparams))
        try:
            result = self.dispatcher.call_sync(self.name, *args, **urlparams)
        except RpcException as e:
            raise falcon.HTTPBadRequest(e.message, str(e))
        return result


class Resource(object):

    name = None
    parent = None
    params_type = None

    get = None
    post = None
    put = None
    delete = None

    subresources = None

    def __init__(self, rest, parent=None):
        self.rest = rest
        self.children = []

        if parent is not None:
            parent.add_child(self)

        self.rest.api.add_route(self.get_uri(), self)

        if self.subresources is not None:
            for sr in self.subresources:
                sr(rest, parent=self)

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
        assert self.name is not None
        if self.parent is None:
            return '/{0}'.format(self.name)
        return '{0}/{1}'.format(self.parent.get_uri(), self.name)

    def get_path_params(self):
        current = self
        params = {}
        while current is not None:
            if current.params_type is not None:
                params.update(current.params_type)
            current = current.parent
        return params

    def _get_type_name(self, method_op):
        if ':' not in method_op:
            raise falcon.HTTPError(
                falcon.HTTP_500, 'Internal Error', 'No valid operation provided'
            )
        type_, name = method_op.split(':', 1)
        assert type_ in ('task', 'rpc')
        return type_, name

    def do(self, method, req, resp, **urlparams):
        """Runs the corresponding task or RPC for the given method of this
        resource"""
        method_op = getattr(self, method)
        type_, name = self._get_type_name(method_op)

        rv = None
        if type_ == 'task':
            t = Task(self, req.context['client'], method, name=name)
            rv = req.context['result'] = t.run(req, urlparams)['result']
        else:
            r = RPC(self, req.context['client'], method, name=name)
            rv = req.context['result'] = r.run(req, urlparams)

        if method == 'post':
            resp.status = falcon.HTTP_201
        elif method == 'delete':
            resp.status = falcon.HTTP_204
        return rv

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

            response = {
                'description': 'entries to be returned',
            }
            schema = normalize_schema(op.get('result-schema'), self.rest)
            if schema is not None:
                response['schema'] = schema

            rv[i] = {
                'description': op.get('description') or 'UNKNOWN',
                'responses': {
                    code_map[i]: response,
                },
            }

            rv[i]['parameters'] = []

            path_params = self.get_path_params()
            if path_params:
                for pname, ptype in path_params.items():
                    rv[i]['parameters'].append({
                        'name': pname,
                        'in': 'path',
                        'required': True,
                        'type': ptype,
                    })

            if i in ('post', 'put'):

                if type_ == 'task':
                    schema = op.get('schema', [None])
                    if schema:
                        schema = schema[0 if i == 'post' else -1]
                else:
                    schema = op.get('result-schema')
                rv[i]['parameters'].append(
                    {
                        'name': 'data',
                        'in': 'body',
                        'required': True,
                        'schema': normalize_schema(schema, self.rest) or {'type': 'null'},
                    },
                )
        return rv


class ResourceQueryMixin:

    def run_get(self, req, urlparams):
        args = []
        for key, val in req.params.items():
            if '__' in key:
                field, op = key.split('__', 1)
            else:
                field, op = key, '='

            def convert(val):
                if val.isdigit():
                    val = int(val)
                elif val.lower() in ('true', 'false', '0', '1'):
                    if val.lower() in ('true', '1'):
                        val = True
                    elif val.lower() in ('false', '0'):
                        val = False
                return val

            if key in ('limit', 'offset', 'count'):
                urlparams[key] = convert(val)
                continue
            elif key == 'sort':
                urlparams[key] = [convert(v) for v in val.split(',')]
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
            elif val.lower() == 'null':
                val = None
            args.append((field, op, val))

        return [args, urlparams], {}


class EntityResource(Resource, ResourceQueryMixin):

    def do(self, method, req, resp, *args, **kwargs):
        rv = super(EntityResource, self).do(method, req, resp, *args, **kwargs)
        if method == 'post':
            if rv:
                """
                Generally in CRUD a POST will mean item creation and the goal
                here is to return the item created.
                Dispatcher tasks usually return just the `id` so we need to
                query for the object. This is done calling the `get` method.
                """
                typ, name = self._get_type_name(self.get)
                if typ == 'rpc':
                    r = RPC(self, req.context['client'], 'get', name=name)
                    req.context['result'] = r.run(req, {}, get_args=lambda a, b: [[[('id', '=', rv)], {'single': True}], {}])
                else:
                    raise NotImplementedError('{0} not implemented'.format(typ))
            resp.status = falcon.HTTP_201
        return rv

    def run_post(self, req, urlparams):
        args = []
        if 'doc' in req.context:
            args.append(req.context['doc'])
        return args


class ItemResource(Resource):

    name = 'id/{id}'
    params_type = {
        'id': 'integer',
    }

    def run_get(self, req, kwargs):
        id = kwargs['id']
        if id.isdigit():
            id = int(id)
        return [[('id', '=', id)], {'single': True}], {}

    def _run_common(self, req, urlparams):
        """
        Common method to get the id from url and use a first task arg
        """
        args = []
        if 'id' in urlparams:
            args.append(urlparams['id'])
        if 'doc' in req.context:
            args.append(req.context['doc'])
        return args

    def run_post(self, req, urlparams):
        return self._run_common(req, urlparams)

    def run_put(self, req, urlparams):
        return self._run_common(req, urlparams)

    def run_delete(self, req, urlparams):
        return self._run_common(req, urlparams)

    def do(self, method, req, resp, *args, **kwargs):
        rv = super(ItemResource, self).do(method, req, resp, *args, **kwargs)
        if method == 'get':
            if req.context['result'] is None:
                del req.context['result']
                resp.status = falcon.HTTP_404
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

            method_op = 'rpc:{0}.{1}'.format(provider_name, method['name'])

            # Skip method if same as current resource
            # e.g. GET /service/ftp will query service.ftp.get_config
            #      so we don't need a /service/ftp/get_config
            if method_op == self.get:
                continue

            if 'params-schema' not in method or len(method['params-schema']['items']) == 0:
                http_method = 'get'
            else:
                http_method = 'post'

            type('{0}Resource'.format(method['name']), (Resource, ), {
                'name': method['name'],
                http_method: method_op,
            })(rest, parent=self)

    def get_provider_name(self):
        if self.provider:
            return self.provider
        if self.get is not None and self.get.startswith('rpc:'):
            return self.get[4:].rsplit('.', 1)[0]


class SingleItemBase(object):

    name = None
    namespace = None
    resource_class = Resource

    subresources = None

    def __init__(self, rest, dispatcher):

        put = self.get_update_method_name()
        resource = {
            'name': self.name or self.namespace.replace('.', '/'),
            'get': 'rpc:{0}'.format(self.get_retrieve_method_name()),
        }
        if put is not None:
            resource['put'] = 'task:{0}'.format(put)
        resource_obj = type('{0}Resource'.format(self.__class__.__name__), (ProviderMixin, self.resource_class, ), resource)(rest)

        # Instantiate subresources
        # e.g. /resource_name/subresource_name
        if self.subresources is not None:
            for subresource in self.subresources:
                subresource(rest, parent=resource_obj)

    def get_retrieve_method_name(self):
        return '{0}.get_config'.format(self.namespace)

    def get_update_method_name(self):
        return '{0}.update'.format(self.namespace)


class CRUDBase(object):

    name = None
    namespace = None

    entity_class = EntityResource
    item_class = ItemResource

    entity_resources = None
    item_resources = None

    def __init__(self, rest, dispatcher):

        get = self.get_retrieve_method_name()
        post = self.get_create_method_name()
        put = self.get_update_method_name()
        delete = self.get_delete_method_name()

        self.entity = type('{0}EntityResource'.format(self.__class__.__name__), (ProviderMixin, self.entity_class, ), {
            'name': self.name or self.namespace.replace('.', '/'),
            'get': 'rpc:{0}'.format(get) if get else None,
            'post': 'task:{0}'.format(post) if post else None,
        })(rest)
        self.item = type('{0}ItemResource'.format(self.__class__.__name__), (self.item_class, ), {
            'get': 'rpc:{0}'.format(get) if get else None,
            'put': 'task:{0}'.format(put) if put else None,
            'delete': 'task:{0}'.format(delete) if delete else None,
        })(rest, parent=self.entity)

        if self.item_resources is not None:
            for ir in self.item_resources:
                ir(rest, parent=self.item)

        if self.entity_resources is not None:
            for er in self.entity_resources:
                er(rest, parent=self.entity)

    def get_create_method_name(self):
        return '{0}.create'.format(self.namespace)

    def get_retrieve_method_name(self):
        return '{0}.query'.format(self.namespace)

    def get_update_method_name(self):
        return '{0}.update'.format(self.namespace)

    def get_delete_method_name(self):
        return '{0}.delete'.format(self.namespace)
