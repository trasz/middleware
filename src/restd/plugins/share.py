from base import CRUDBase, EntityResource


class ShareSMBEntityResource(EntityResource):

    def run_get(self, req, urlparams):
        """Always inject a type=smb filter to share.query"""
        args, kwargs = super(ShareSMBEntityResource, self).run_get(req, urlparams)
        args[0].append(('type', '=', 'smb'))
        return args, kwargs

    def run_post(self, req, urlparams):
        data = req.context['doc']
        data['type'] = 'smb'
        if 'properties' in data:
            data['properties']['type'] = 'service-smb'
        return [data]


class ShareSMBResource(CRUDBase):
    name = 'share/smb'
    namespace = 'share'
    entity_class = ShareSMBEntityResource


def _init(rest):
    rest.register_crud(ShareSMBResource)
