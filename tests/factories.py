from redis import Redis
from tests import settings


class StoredObjectMock(object):
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs.get(key))


def create_device():
    device_data = {
        'owner_id': 5, 'tags': ['a', 'abab', 'ñ'], 'organization_id': 15,
        'created_at': 191919881, 'context': {'lat': 12.98, 'lng': -199.029},
        'label': 'device_label', 'name': 'device_name',
        'ubi_context': {'_abba': 192}, 'description': 'Nice device used to do nice staff',
        'state': 0, 'id': 'f' * 24,
        'enabled': True, 'last_activity': 188826663,
        'variables': ['a' * 24, 'b' * 24, 'c' * 24]}
    return StoredObjectMock(**device_data)


def create_variable():
    variable_data = {
        'derived_expr': '{{abc * 10 + cos(z)}}', 'tags': ['a', 'b', 'x'],
        'device_id': 'f' * 24, 'type': 2,
        'created_at': 1921882882, 'properties': {'max': 10, 'min': 7},
        'label': 'variable_label', 'name': 'variable_name',
        'icon': 'variable_icon', 'description': 'variable description',
        'state': 1, 'unit': 'Meters', 'id': 'f' * 24,
        'last_value': {'value': 21.12, 'context': {'lat': 1.1, 'lng': 12}},
        'last_activity': 363663663}
    return StoredObjectMock(**variable_data)


def create_business_account():
    business_account_data = {
        'id': 10, 'owner_id': 5, 'is_active': True, 'date_created': 63363663,
        'balance': 1881,
        'extra_costs': {'sms': 10}, 'prices': {'sms': 12},
        'initial_free_items': {'sms': 12}, 'limits': {'sms': 1000},
        'one_time_costs': {'staff': 10},
        'business_type': 1, 'last_activity': 12929992, 'trial_end_timestamp_utc': 199929,
        'invoice_to': 'jdaaa2009@gmail.com',
        'custom_note': 'This is a custom note.', 'plan': 'This is the plan',
        'from_email': 'jdaaa2009@gmail.com'}
    return StoredObjectMock(**business_account_data)


def get_redis_connection():
    return Redis(db=settings.EVENTS_REDIS_DATABASE_DB,
                 host=settings.EVENTS_REDIS_DATABASE_HOST,
                 port=settings.EVENTS_REDIS_DATABASE_PORT,
                 password=settings.EVENTS_REDIS_DATABASE_PASSWORD)
