from base import Resource, SingleItemBase


class ApplyResource(Resource):
    name = 'apply'
    post = 'task:update.apply'


class CheckResource(Resource):
    name = 'check'
    post = 'task:update.check'


class CheckFetchResource(Resource):
    name = 'checkfetch'
    post = 'task:update.checkfetch'


class DownloadResource(Resource):
    name = 'download'
    post = 'task:update.download'


class UpdateNowResource(Resource):
    name = 'updatenow'
    post = 'task:update.updatenow'


class VerifyResource(Resource):
    name = 'verify'
    post = 'task:update.verify'


class UpdateSingleItem(SingleItemBase):
    namespace = 'update'
    subresources = (
        ApplyResource,
        CheckResource,
        CheckFetchResource,
        DownloadResource,
        UpdateNowResource,
        VerifyResource,
    )


def _init(rest):
    rest.register_singleitem(UpdateSingleItem)
