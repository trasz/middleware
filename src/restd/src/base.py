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

    def run(self, *args, **kwargs):
        try:
            result = self.dispatcher.call_task_sync(self.name, *args, **kwargs)
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

            def on_method(req, resp):
                return do(method, req, resp)

            return on_method
        return object.__getattribute__(self, attr)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def get_uri(self):
        if self.parent is None:
            return '/{0}'.format(self.name)
        return '{0}/{1}'.format(self.parent.get_uri(), self.name)

    def do(self, method, req, resp, **kwargs):
        method_op = getattr(self, method)
        if ':' not in method_op:
            raise falcon.HTTPError(
                falcon.HTTP_500, 'Internal Error', 'No valid operation provided'
            )
        type_, name = method_op.split(':', 1)
        assert type_ in ('task', 'rpc')

        if type_ == 'task':
            t = Task(self, req.context['client'], name=name)
            t.run(req.context['doc'])


class EntityResource(object):

    def __init__(self, rest, dispatcher, crud):
        self.rest = rest
        self.dispatcher = dispatcher
        self.crud = crud

    def get_uri(self):
        return self.crud.get_uri()

    def get_path_item(self):
        get = self.rest._rpcs.get(self.crud.get_retrieve_method_name())
        post = self.rest._tasks.get(self.crud.get_create_method_name())
        return {
            'get': {
                'description': get['description'] if get else None,
                'responses': {
                    '200': {
                        'description': 'entries to be returned',
                        'schema': normalize_schema(get['result-schema']) if get else None,
                    }
                },
                'produces': [
                    'application/xml',
                    'application/json'
                ],
            },
            'post': {
                'description': post['description'] if post else 'Create a new entry',
                'responses': {
                    '200': {
                        'description': 'new entry returned',
                        'schema': normalize_schema(get['result-schema']['anyOf'][-1]) if post else None,
                    }
                },
                'parameters': [
                    {
                        'name': 'data',
                        'in': 'body',
                        'required': True,
                        'schema': normalize_schema(post['schema']),
                    },
                ],
                'produces': [
                    'application/xml',
                    'application/json'
                ],
            },
        }

    def on_get(self, req, resp):
        result = []
        for entry in req.context['client'].call_sync(self.crud.get_retrieve_method_name()):
            result.append(entry)
        req.context['result'] = result

    def on_post(self, req, resp):
        try:
            result = req.context['client'].call_task_sync(self.crud.get_create_method_name(), req.context['doc'])
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
        if result['result']:
            entry = req.context['client'].call_sync(self.crud.get_retrieve_method_name(), [('id', '=', result['result'])], {'single': True})
            if entry is None:
                raise falcon.HTTPNotFound
            req.context['result'] = entry


class ItemResource(object):

    def __init__(self, rest, dispatcher, crud):
        self.rest = rest
        self.dispatcher = dispatcher
        self.crud = crud

    def get_uri(self):
        return '{0}/{{id}}'.format(self.crud.get_uri())

    def get_path_item(self):
        get = self.rest._rpcs.get(self.crud.get_retrieve_method_name())
        put = self.rest._tasks.get(self.crud.get_update_method_name())
        delete = self.rest._tasks.get(self.crud.get_delete_method_name())
        return {
            'get': {
                'description': get['description'] if get else None,
                'responses': {
                    '200': {
                        'description': 'entries to be returned',
                        'schema': normalize_schema(get['result-schema']['anyOf'][-1]) if get else None,
                    }
                },
                'produces': [
                    'application/xml',
                    'application/json'
                ],
            },
            'put': {
                'description': put['description'] if put else 'Update the entry',
                'responses': {
                    '200': {
                        'description': 'Entry has been updated',
                        'schema': normalize_schema(put['schema']) if put else None,
                    }
                }
            },
            'delete': {
                'description': delete['description'] if delete else 'Delete the entry',
                'responses': {
                    '200': {
                        'description': 'Entry has been deleted',
                        'schema': normalize_schema(delete['schema']) if delete else None,
                    }
                }
            },
            'parameters': [
                {
                    'name': 'id',
                    'in': 'path',
                    'required': True,
                    'type': 'integer',
                    'format': 'int64',
                },
            ],
        }

    def get_entry(self, client, id):
        entry = client.call_sync(self.crud.get_retrieve_method_name(), [('id', '=', int(id))], {'single': True})
        if entry is None:
            raise falcon.HTTPNotFound
        return entry

    def on_get(self, req, resp, id):
        req.context['result'] = self.get_entry(req.context['client'], id)

    def on_put(self, req, resp, id):
        entry = self.get_entry(id)
        try:
            result = req.context['client'].call_task_sync(self.crud.get_update_method_name(), [int(id), req.context['doc']])
        except RpcException as e:
            raise falcon.HTTPBadRequest(e.message, str(e))
        if result['state'] != 'FINISHED':
            if result['error']:
                title = result['error']['type']
                message = result['error']['message']
            else:
                title = 'UnknownError'
                message = 'Failed to update, check task #{0}'.format(result['id'])
            raise falcon.HTTPBadRequest(title, message)

    def on_delete(self, req, resp, id):
        entry = self.get_entry(id)
        try:
            result = req.context['client'].call_task_sync(self.crud.get_delete_method_name(), [int(id)])
        except RpcException as e:
            raise falcon.HTTPBadRequest(e.message, str(e))
        if result['state'] != 'FINISHED':
            if result['error']:
                title = result['error']['type']
                message = result['error']['message']
            else:
                title = 'UnknownError'
                message = 'Failed to delete, check task #{0}'.format(result['id'])
            raise falcon.HTTPBadRequest(title, message)


class CRUDBase(object):

    entity_class = EntityResource
    item_class = ItemResource
    namespace = None

    def __init__(self, rest, dispatcher):
        self.entity = self.entity_class(rest, dispatcher, self)
        self.item = self.item_class(rest, dispatcher, self)

        rest.api.add_route(self.entity.get_uri(), self.entity)
        rest.api.add_route(self.item.get_uri(), self.item)

    def add_child(self, child):
        child.parent = self

    def get_uri(self):
        return '/{0}'.format(self.namespace)

    def get_create_method_name(self):
        return '{0}.create'.format(self.namespace)

    def get_retrieve_method_name(self):
        return '{0}.query'.format(self.namespace)

    def get_update_method_name(self):
        return '{0}.update'.format(self.namespace)

    def get_delete_method_name(self):
        return '{0}.delete'.format(self.namespace)

    def get_paths(self):
        return {
            self.entity.get_uri(): self.entity.get_path_item(),
            self.item.get_uri(): self.item.get_path_item()
        }
