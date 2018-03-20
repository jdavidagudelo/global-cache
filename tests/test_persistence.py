import unittest
from global_cache import utils_persistence
from tests import utils_aerospike
from tests import utils_redis
from tests import settings

global_cache = utils_redis.get_global_cache_connection()
redis = utils_redis.get_redis_connection()


class StoredObjectMock(object):
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs.get(key))


class TestDerivedExpression(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        global_cache.flushdb()
        redis.flushdb()
        cache = utils_persistence.AerospikeCache(
            client=utils_aerospike.get_aerospike_client(),
            default_namespace=settings.AEROSPIKE_DEFAULT_NAMESPACE,
            default_set_name=settings.AEROSPIKE_DEFAULT_SET_NAME)
        cache.client.truncate(settings.AEROSPIKE_DEFAULT_NAMESPACE, settings.AEROSPIKE_DEFAULT_SET_NAME, 0)

    def test_aerospike(self):
        client = utils_persistence.AerospikeCache(
            client=utils_aerospike.get_aerospike_client(),
            default_namespace=settings.AEROSPIKE_DEFAULT_NAMESPACE,
            default_set_name=settings.AEROSPIKE_DEFAULT_SET_NAME).client
        key = ('test', 'set', 'set_key')
        udf_bin = 'a'
        client.udf_put('../lua_scripts/set.lua')
        expected_set = {17, 18, 19, 20, 21, 11, 7, 8, 9}
        s1 = {17, 18, 19, 20, 21}
        s2 = {11, 7, 8, 9}
        client.apply(key, 'set', 'unique_set_write_many', [udf_bin, [x for x in s1]])
        client.apply(key, 'set', 'unique_set_write_many', [udf_bin, [x for x in s2]])
        result = client.apply(key, 'set', 'unique_set_scan', ['a'])
        self.assertEqual(set(['{0}'.format(e) for e in expected_set]), set(result))
        client.apply(key, 'set', 'unique_set_remove_many', [udf_bin, [x for x in s1]])
        client.apply(key, 'set', 'unique_set_remove_many', [udf_bin, [x for x in s2]])
        result = client.apply(key, 'set', 'unique_set_scan', ['a'])
        self.assertEqual(len(result), 0)

    def test_aerospike_cache(self):
        cache = utils_persistence.AerospikeCache(
            client=utils_aerospike.get_aerospike_client(),
            default_namespace=settings.AEROSPIKE_DEFAULT_NAMESPACE,
            default_set_name=settings.AEROSPIKE_DEFAULT_SET_NAME)
        cache.save_udf('../lua_scripts/set.lua')
        key = 'set_key'
        expected_set = {17, 18, 19, 20, 21, 11, 7, 8, 9}
        s1 = {17, 18, 19, 20, 21}
        s2 = {11, 7, 8, 9}
        cache.set_add_many(key, [x for x in s1])
        cache.set_add_many(key, [x for x in s2])
        result = cache.set_scan(key)
        self.assertEqual(set(['{0}'.format(e) for e in expected_set]), set(result))
        cache.set_remove_many(key, [x for x in s1])
        cache.set_remove_many(key, [x for x in s2])
        result = cache.set_scan(key)
        self.assertEqual(len(result), 0)

    def test_redis_cache(self):
        cache = utils_persistence.RedisCache(utils_redis.get_redis_connection())
        key = 'set_key'
        cache.set_clear(key)
        expected_set = {17, 18, 19, 20, 21, 11, 7, 8, 9}
        s1 = {17, 18, 19, 20, 21}
        s2 = {11, 7, 8, 9}
        cache.set_add_many(key, [x for x in s1])
        cache.set_add_many(key, [x for x in s2])
        result = cache.set_scan(key)
        self.assertEqual(set(['{0}'.format(e).encode('utf-8') for e in expected_set]), set(result))
        cache.set_remove_many(key, [x for x in s1])
        cache.set_remove_many(key, [x for x in s2])
        result = cache.set_scan(key)
        self.assertEqual(len(result), 0)

    def test_key_value_aerospike(self):
        cache = utils_persistence.AerospikeCache(
            client=utils_aerospike.get_aerospike_client(),
            default_namespace=settings.AEROSPIKE_DEFAULT_NAMESPACE,
            default_set_name=settings.AEROSPIKE_DEFAULT_SET_NAME)
        key = 'key'
        expected = 'f' * 24
        cache.put_value(key, expected)
        result = cache.get_value(key)
        self.assertEqual(result, expected)

    def test_map_value_aerospike(self):
        cache = utils_persistence.AerospikeCache(
            client=utils_aerospike.get_aerospike_client(),
            default_namespace=settings.AEROSPIKE_DEFAULT_NAMESPACE,
            default_set_name=settings.AEROSPIKE_DEFAULT_SET_NAME)
        key = 'key'
        map_key = 'map_key'
        expected = 'f' * 24
        cache.map_put_value(key, map_key, expected)
        result = cache.map_get_value(key, map_key)
        self.assertEqual(result, expected)

    def test_map_increment_aerospike(self):
        cache = utils_persistence.AerospikeCache(
            client=utils_aerospike.get_aerospike_client(),
            default_namespace=settings.AEROSPIKE_DEFAULT_NAMESPACE,
            default_set_name=settings.AEROSPIKE_DEFAULT_SET_NAME)
        key = 'key'
        map_key = 'map_key'
        expected = 10
        cache.map_increment(key, map_key, 5)
        cache.map_increment(key, map_key, 5)
        result = cache.map_get_value(key, map_key)
        self.assertEqual(result, expected)

    def test_key_value_redis(self):
        cache = utils_persistence.RedisCache(utils_redis.get_redis_connection())
        key = 'key'
        expected = 'f' * 24
        cache.put_value(key, expected)
        result = cache.get_value(key)
        self.assertEqual(result, expected)

    def test_map_value_redis(self):
        cache = utils_persistence.RedisCache(redis)
        key = 'key'
        map_key = 'map_key'
        expected = 'f' * 24
        cache.map_put_value(key, map_key, expected)
        result = cache.map_get_value(key, map_key)
        self.assertEqual(result, expected)

    def test_map_increment_redis(self):
        cache = utils_persistence.RedisCache(utils_redis.get_redis_connection())
        key = 'key'
        map_key = 'map_key'
        expected = 10
        cache.map_increment(key, map_key, 5)
        cache.map_increment(key, map_key, 5)
        result = cache.map_get_value(key, map_key)
        self.assertEqual(result, '{0}'.format(expected))
