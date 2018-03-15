import unittest
from tests import settings
import time
from global_messaging import utils_messaging


class TestGlobalMessaging(unittest.TestCase):
    def setUp(self):
        self.message_sent = False

    def tearDown(self):
        pass

    def test_basic_message(self):
        client = utils_messaging.MessageBrokerClientStomp(
            host_and_ports=settings.STOMP_HOST_AND_PORTS,
            heartbeats=settings.STOMP_HEARTBEATS,
            user=settings.STOMP_SERVER_USER,
            password=settings.STOMP_SERVER_PASSWORD)
        destination = 'topic1'
        client_id = 'client_id'
        message = 'message'

        def message_received_function(**kwargs):
            self.assertEqual(destination, kwargs.get('destination'))
            self.assertEqual(message, kwargs.get('message'))
            self.message_sent = True

        client.add_message_listener(message_received_function, 'xxx')
        client.subscribe_message(destination, client_id)
        client.send_message(destination, message)
        time.sleep(0.5)
        client.stop()
        client.disconnect()
        self.assertEqual(self.message_sent, True)
