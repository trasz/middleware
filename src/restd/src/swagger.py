import json


def normalize_schema(obj):
     if isinstance(obj, dict):
         if '$ref' in obj:
             ref = obj['$ref']
             if not ref.startswith('#/definitions/'):
                 obj['$ref'] = '#/definitions/{0}'.format(ref)
         for key in obj:
             normalize_schema(obj[key])
     elif isinstance(obj, (list, tuple)):
         for i in obj:
             normalize_schema(i)
     return obj


class SwaggerResource(object):

    def __init__(self, rest):
        self.rest = rest

    def on_get(self, req, resp):

        result = {
            'swagger': '2.0',
            'info': {
                'title': 'FreeNAS RESTful API',
                'version': '1.0',
            },
            'paths': {},
            'definitions': normalize_schema(self.rest._schemas['definitions']),
        }

        for crud in self.rest._cruds:
            result['paths'].update(crud.get_paths())

        resp.body = json.dumps(result)
