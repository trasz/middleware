from base import CRUDBase


class KerberosKeytabCRUD(CRUDBase):
    namespace = 'kerberos.keytab'


class KerberosRealmCRUD(CRUDBase):
    namespace = 'kerberos.realm'


def _init(rest):
    rest.register_crud(KerberosKeytabCRUD)
    rest.register_crud(KerberosRealmCRUD)
