from base import Resource


class SaveToFileResource(Resource):
    name = 'debug/save_to_file'
    post = 'task:debug.save_to_file'

    def run_post(self, req, urlparams):
        return req.context['doc']


def _init(rest):
    rest.register_resource(SaveToFileResource)
