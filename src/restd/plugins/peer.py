from base import CRUDBase


class PeerCRUD(CRUDBase):
    namespace = 'peer'


class PeerReplicationCRUD(CRUDBase):
    namespace = 'peer.replication'

    def get_retrieve_method_name(self):
        return None


def _init(rest):
    rest.register_crud(PeerCRUD)
    rest.register_crud(PeerReplicationCRUD)
