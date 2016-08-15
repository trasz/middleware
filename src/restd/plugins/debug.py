from base import Resource


class SaveToFileResource(Resource):
    name = 'debug.save_to_file'
    post = 'task:debug.save_to_file'


def _init(rest):
    rest.register_resource(SaveToFileResource)
