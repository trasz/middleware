import logging

logger = logging.getLogger('swagger')


def normalize_schema(obj, rest=None):
    if isinstance(obj, dict):
        if '$ref' in obj:
            ref = obj['$ref']
            if not ref.startswith('#/definitions/'):
                if rest:
                    rest._used_schemas.add(ref)
                obj['$ref'] = '#/definitions/{0}'.format(ref)
        for key in obj:
            normalize_schema(obj[key], rest)
    elif isinstance(obj, (list, tuple)):
        for i in obj:
            normalize_schema(i, rest)
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
            try:
                result['paths'][path] = doc()
            except:
                logger.warn('Failed to generate swagger doc for {0}'.format(path), exc_info=True)

        """
        Get the schema definitions which are referenced somewhere
        in the public API.
        To do that we go through all used schemas and resolve it until no
        more schemas are found.
        """
        # binary type is not json schema compliant so we emulate it
        definitions = {
            'binary-type': {
                'type': 'object',
                'properties': {
                    '$binary': {'type': 'string'},
                }
            }
        }
        schemas_done = set()
        while len(schemas_done) != len(self.rest._used_schemas):
            for name in (self.rest._used_schemas - schemas_done):
                schema = self.rest._schemas['definitions'].get(name)
                if schema:
                    definitions[name] = normalize_schema(schema, self.rest)
                else:
                    print("not found", name)
                schemas_done.add(name)
        result['definitions'] = definitions

        req.context['result'] = result
