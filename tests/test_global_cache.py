import unittest
from tests.factories import get_redis_connection
from tests import factories
from global_cache import utils_global_cache


redis_connection = get_redis_connection()


class TestVariableEntity(unittest.TestCase):
    def setUp(self):
        redis_connection.flushdb()
        self.variable = factories.create_variable()
        self.device = factories.create_device()
        self.business_account = factories.create_business_account()

    def tearDown(self):
        redis_connection.flushdb()

    def test_save_variable(self):
        variable_cache = utils_global_cache.Variable(self.variable, connection=redis_connection)
        variable_cache.save()
        for attribute in variable_cache.attributes:
            self.assertEqual(getattr(variable_cache, attribute),
                             variable_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.variable, getattr(self.variable, attribute, None)))
        device_by_label_cache = utils_global_cache.VariableByLabel(
            primary_key=self.variable.label, variable=self.variable, connection=redis_connection)
        device_by_label_cache.save()
        data = variable_cache.get_all_attributes()
        for attribute in variable_cache.attributes:
            self.assertEqual(data.get(attribute).get('value'),
                             variable_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.variable, getattr(self.variable, attribute, None)))
        data = device_by_label_cache.get_all_attributes_nested('id')
        for attribute in variable_cache.attributes:
            self.assertEqual(data.get(attribute).get('value'),
                             variable_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.variable, getattr(self.variable, attribute, None)))
        for attribute in variable_cache.attributes:
            self.assertEqual(getattr(device_by_label_cache, attribute),
                             variable_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.variable, getattr(self.variable, attribute, None)))
        variable_cache.delete()
        for attribute in variable_cache.attributes:
            self.assertEqual(getattr(variable_cache, attribute),
                             None)

    def test_save_device(self):
        device_cache = utils_global_cache.Device(self.device, connection=redis_connection)
        self.assertEqual(device_cache.hash_key, '{0}:{1}:{2}:{3}'.format(
            'INDUSTRIAL', 'device', 'id', self.device.id))
        device_cache.save()
        for attribute in device_cache.attributes:
            self.assertEqual(getattr(device_cache, attribute),
                             device_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.device, getattr(self.device, attribute, None)))
        device_by_label_cache = utils_global_cache.DeviceByLabel(
            primary_key=self.device.label, device=self.device, connection=redis_connection)
        device_by_label_cache.save()
        data = device_cache.get_all_attributes()
        for attribute in device_cache.attributes:
            self.assertEqual(data.get(attribute).get('value'),
                             device_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.device, getattr(self.device, attribute, None)))
        data = device_by_label_cache.get_all_attributes_nested('id')
        for attribute in device_cache.attributes:
            self.assertEqual(data.get(attribute).get('value'),
                             device_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.device, getattr(self.device, attribute, None)))
        for attribute in device_cache.attributes:
            self.assertEqual(getattr(device_by_label_cache, attribute),
                             device_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.device, getattr(self.device, attribute, None)))

        device_cache.delete()
        for attribute in device_cache.attributes:
            self.assertEqual(getattr(device_cache, attribute),
                             None)

    def test_save_business_account(self):
        business_account_cache = utils_global_cache.BusinessAccount(
            self.business_account, connection=redis_connection)
        business_account_cache.save()
        for attribute in business_account_cache.attributes:
            self.assertEqual(getattr(business_account_cache, attribute),
                             business_account_cache.attributes_mapper.get(attribute, lambda b, v: v)(
                                 self.business_account, getattr(self.business_account, attribute, None)))
        business_account_cache.delete()
        for attribute in business_account_cache.attributes:
            self.assertEqual(getattr(business_account_cache, attribute),
                             None)
