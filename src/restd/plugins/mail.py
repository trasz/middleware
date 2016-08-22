from base import SingleItemBase


class MailSingleItem(SingleItemBase):
    namespace = 'mail'


def _init(rest):
    rest.register_singleitem(MailSingleItem)
