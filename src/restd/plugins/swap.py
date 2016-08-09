from base import Resource


class SwapInfoResource(Resource):
    name = 'swap/info'
    get = 'rpc:swap.info'


def _init(rest):
    rest.register_resource(SwapInfoResource)
