from redis import Redis
from tests import settings


def get_redis_connection():
    return Redis(db=settings.REDIS_DATABASE_DB,
                 host=settings.REDIS_DATABASE_HOST,
                 port=settings.REDIS_DATABASE_PORT,
                 password=settings.REDIS_DATABASE_PASSWORD)


def get_global_cache_connection():
    return Redis(db=settings.GLOBAL_CACHE_REDIS_DATABASE_DB,
                 host=settings.GLOBAL_CACHE_REDIS_DATABASE_HOST,
                 port=settings.GLOBAL_CACHE_REDIS_DATABASE_PORT,
                 password=settings.GLOBAL_CACHE_REDIS_DATABASE_PASSWORD)
