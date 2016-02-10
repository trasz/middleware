import falcon

from freenas.dispatcher.rpc import RpcException

from swagger import normalize_schema


class EntityResource(object):

     def __init__(self, rest, dispatcher, namespace):
         self.rest = rest
         self.dispatcher = dispatcher
         self.namespace = namespace

     def get_path(self):
         return '/{0}'.format(self.namespace)

     def get_path_item(self):
         get = self.rest._rpcs.get('{0}.query'.format(self.namespace))
         post = self.rest._tasks.get('{0}.create'.format(self.namespace))
         return {
             'get': {
                 'description': get['description'] if get else None,
                 'responses': {
                     '200': {
                         'description': 'entries to be returned',
                         'schema': normalize_schema(get['result-schema']) if get else None,
                     }
                 }
             },
             'post': {
                 'description': post['description'] if post else 'Create a new entry',
                 'responses': {
                     '200': {
                         'description': 'new entry returned',
                         'schema': normalize_schema(post['schema']) if post else None,
                     }
                 }
             },
         }

     def on_get(self, req, resp):
         result = []
         for entry in req.context['client'].call_sync('{0}.query'.format(self.namespace)):
             if 'created_at' in entry:
                 entry['created_at'] = str(entry['created_at'])
             if 'updated_at' in entry:
                 entry['updated_at'] = str(entry['created_at'])
             result.append(entry)
         req.context['result'] = result

     def on_post(self, req, resp):
         try:
             result = req.context['client'].call_task_sync('{0}.create'.format(self.namespace), req.context)
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
             entry = req.context['client'].call_sync('{0}.query'.format(self.namespace), [('id', '=', result['result'])], {'single': True})
             if entry is None:
                 raise falcon.HTTPNotFound
             if 'created_at' in entry:
                 entry['created_at'] = str(entry['created_at'])
             if 'updated_at' in entry:
                 entry['updated_at'] = str(entry['updated_at'])
             req.context['result'] = entry


class ItemResource(object):

     def __init__(self, rest, dispatcher, namespace):
         self.rest = rest
         self.dispatcher = dispatcher
         self.namespace = namespace

     def get_path(self):
         return '/{0}/{{id}}'.format(self.namespace)

     def get_path_item(self):
         get = self.rest._rpcs.get('{0}.query'.format(self.namespace))
         delete = self.rest._tasks.get('{0}.delete'.format(self.namespace))
         return {
             'get': {
                 'description': get['description'] if get else None,
                 'responses': {
                     '200': {
                         'description': 'entries to be returned',
                         'schema': normalize_schema(get['result-schema']['anyOf'][-1]) if get else None,
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

     def on_get(self, req, resp, id):
         entry = req.context['client'].call_sync('{0}.query'.format(self.namespace), [('id', '=', int(id))], {'single': True})
         if entry is None:
             raise falcon.HTTPNotFound
         if 'created_at' in entry:
             entry['created_at'] = str(entry['created_at'])
         if 'updated_at' in entry:
             entry['updated_at'] = str(entry['updated_at'])
         req.context['result'] = entry

     def on_delete(self, req, resp, id):
         entry = req.context['client'].call_sync('{0}.query'.format(self.namespace), [('id', '=', int(id))], {'single': True})
         if entry is None:
             raise falcon.HTTPNotFound
         try:
             result = req.context['client'].call_task_sync('{0}.delete'.format(self.namespace), [int(id)])
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

    namespace = None

    def __init__(self, rest, dispatcher):
        self.entity = EntityResource(rest, dispatcher, self.namespace)
        self.item = ItemResource(rest, dispatcher, self.namespace)

        rest.api.add_route(self.entity.get_path(), self.entity)
        rest.api.add_route(self.item.get_path(), self.item)

    def get_paths(self):
        return {
            self.entity.get_path(): self.entity.get_path_item(),
            self.item.get_path(): self.item.get_path_item()
        }
