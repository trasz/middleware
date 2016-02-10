import json


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
        }

        for crud in self.rest._cruds:
            result['paths'].update(crud.get_paths())

        resp.body = json.dumps(result)
