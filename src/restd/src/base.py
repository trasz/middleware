import falcon

from freenas.dispatcher.rpc import RpcException

from swagger import normalize_schema


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
                        'schema': normalize_schema(post['schema']) if post else None,
                    }
                },
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
            result = req.context['client'].call_task_sync(self.crud.get_create_method_name(), req.context)
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

    def get_entry(self, id):
        entry = req.context['client'].call_sync(self.crud.get_retrieve_method_name(), [('id', '=', int(id))], {'single': True})
        if entry is None:
            raise falcon.HTTPNotFound
        return entry

    def on_get(self, req, resp, id):
        req.context['result'] = self.get_entry(id)

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
