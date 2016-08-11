from base import ProviderMixin, Resource, SingleItemBase


class CheckResource(Resource):
    name = 'check'
    post = 'task:update.check'


class UpdateSingleItem(SingleItemBase):
    namespace = 'update'
    subresources = (
        CheckResource,
    )


def _init(rest):
    rest.register_singleitem(UpdateSingleItem)
