from base import Resource


class FilesystemListDirResource(Resource):
    name = 'filesystem/list_dir'
    post = 'rpc:filesystem.list_dir'


class FilesystemStatResource(Resource):
    name = 'filesystem/stat'
    post = 'rpc:filesystem.stat'


class FilesystemGetOpenFilesResource(Resource):
    name = 'filesystem/get_open_files'
    post = 'rpc:filesystem.get_open_files'


class FileSetPermissionsResource(Resource):
    name = 'file/set_permissions'
    post = 'task:file.set_permissions'

    def run_post(self, req, urlparams):
        return req.context['doc']


def _init(rest):
    # TODO: filesystem.{download,upload}
    rest.register_resource(FilesystemListDirResource)
    rest.register_resource(FilesystemStatResource)
    rest.register_resource(FilesystemGetOpenFilesResource)

    rest.register_resource(FileSetPermissionsResource)
