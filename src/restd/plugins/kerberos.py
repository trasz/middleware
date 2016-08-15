from base import CRUDBase


class KerberosKeytabResource(CRUDBase):
    namespace = 'kerberos.keytab'


class KerberosRealmResource(CRUDBase):
    namespace = 'kerberos.realm'


def _init(rest):
    rest.register_crud(KerberosKeytabResource)
    rest.register_crud(KerberosRealmResource)
