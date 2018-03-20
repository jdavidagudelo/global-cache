import aerospike
from tests import settings


def get_aerospike_client() -> aerospike.Client:
    config = {
        'hosts': settings.AEROSPIKE_HOSTS,
        'policies': {
            'write': settings.AEROSPIKE_WRITE_POLICIES,
            'read': settings.AEROSPIKE_READ_POLICIES
        }
    }
    client = aerospike.client(config).connect()
    return client
