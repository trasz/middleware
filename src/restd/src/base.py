import falcon
import json


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
             entry['updated_at'] = str(entry['created_at'])
         resp.body = json.dumps(entry)

     def on_delete(self, req, resp, id):
         entry = self.dispatcher.call_sync('{0}.query'.format(self.namespace), [('id', '=', int(id))], {'single': True})
         if entry is None:
             raise falcon.HTTPNotFound


class CRUDBase(object):

    namespace = None

    def __init__(self, rest, dispatcher):
        entity = EntityResource(dispatcher, self.namespace)
        item = ItemResource(dispatcher, self.namespace)

        rest.api.add_route('/{0}'.format(self.namespace), entity)
        rest.api.add_route('/{0}/{{id}}'.format(self.namespace), item)
