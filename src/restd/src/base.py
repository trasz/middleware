import falcon
import json

from freenas.dispatcher.rpc import RpcException


class EntityResource(object):

     def __init__(self, dispatcher, namespace):
         self.dispatcher = dispatcher
         self.namespace = namespace

     def on_get(self, req, resp):
         result = []
         for entry in self.dispatcher.call_sync('{0}.query'.format(self.namespace)):
             if 'created_at' in entry:
                 entry['created_at'] = str(entry['created_at'])
             if 'updated_at' in entry:
                 entry['updated_at'] = str(entry['created_at'])
             result.append(entry)
         resp.body = json.dumps(result)

     def on_post(self, req, resp):
         try:
             result = self.dispatcher.call_task_sync('{0}.create'.format(self.namespace), req.context)
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
             entry = self.dispatcher.call_sync('{0}.query'.format(self.namespace), [('id', '=', result['result'])], {'single': True})
             if entry is None:
                 raise falcon.HTTPNotFound
             if 'created_at' in entry:
                 entry['created_at'] = str(entry['created_at'])
             if 'updated_at' in entry:
                 entry['updated_at'] = str(entry['updated_at'])
             resp.body = json.dumps(entry)


class ItemResource(object):

     def __init__(self, dispatcher, namespace):
         self.dispatcher = dispatcher
         self.namespace = namespace

     def on_get(self, req, resp, id):
         entry = self.dispatcher.call_sync('{0}.query'.format(self.namespace), [('id', '=', int(id))], {'single': True})
         if entry is None:
             raise falcon.HTTPNotFound
         if 'created_at' in entry:
             entry['created_at'] = str(entry['created_at'])
         if 'updated_at' in entry:
             entry['updated_at'] = str(entry['updated_at'])
         resp.body = json.dumps(entry)

     def on_delete(self, req, resp, id):
         entry = self.dispatcher.call_sync('{0}.query'.format(self.namespace), [('id', '=', int(id))], {'single': True})
         if entry is None:
             raise falcon.HTTPNotFound
         try:
             result = self.dispatcher.call_task_sync('{0}.delete'.format(self.namespace), [int(id)])
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
        entity = EntityResource(dispatcher, self.namespace)
        item = ItemResource(dispatcher, self.namespace)

        rest.api.add_route('/{0}'.format(self.namespace), entity)
        rest.api.add_route('/{0}/{{id}}'.format(self.namespace), item)
