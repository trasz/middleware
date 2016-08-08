from base import SingleItemBase


class MailResource(SingleItemBase):
    namespace = 'mail'


def _init(rest):
    rest.register_singleitem(MailResource)
