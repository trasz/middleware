from base import CRUDTestCase, SingleItemTestCase


class MailTestCase(SingleItemTestCase):
    name = 'mail'

    def test_020_retrieve(self):
        r = super(MailTestCase, self).test_020_retrieve()

    def get_update_data(self):
        return {
            'server': 'mail.ixsystems.com',
        }
