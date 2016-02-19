

def normalize_schema(obj):
    if isinstance(obj, dict):
        if 'anyOf' in obj:
            # FIXME: Wrong, OpenAPI does not support it, need workaround
            obj = obj['anyOf'][0]
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
            'schemes': ['http', 'https'],
            'basePath': '/api/v2.0',
            'consumes': [
                'application/json',
            ],
            'produces': [
                'application/json',
            ],
            'paths': {},
            'definitions': normalize_schema(self.rest._schemas['definitions']),
        }

        look = []
        for c in self.rest.api._router._roots:
            look.append(('/{0}'.format(c.raw_segment), c))
        while len(look) > 0:
            path, current = look.pop()

            for c in current.children:
                look.append(('{0}/{1}'.format(path, c.raw_segment), c))

            doc = getattr(current.resource, 'doc', None)
            if doc is None:
                continue
            result['paths'][path] = doc()

        req.context['result'] = result
