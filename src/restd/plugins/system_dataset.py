from base import Resource


class SystemDataset(Resource):
    name = 'system_dataset'
    get = 'rpc:system_dataset.status'


def _init(rest):
    rest.register_resource(SystemDataset)
