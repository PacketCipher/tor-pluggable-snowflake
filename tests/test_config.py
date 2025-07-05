import unittest
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client_py.config import parse_args, ClientConfig, DEFAULT_SNOWFLAKE_CAPACITY

class TestConfig(unittest.TestCase):

    def test_default_values(self):
        # Test with no arguments, should get defaults
        with patch('sys.argv', ['prog_name']): # sys.argv[0] is program name
            config = parse_args()

        self.assertEqual(config.ice_servers, [])
        self.assertIsNone(config.broker_url)
        self.assertEqual(config.front_domains, [])
        self.assertIsNone(config.ampcache_url)
        self.assertIsNone(config.sqs_queue_url)
        self.assertIsNone(config.sqs_creds_str)
        self.assertIsNone(config.log_file)
        self.assertFalse(config.log_to_state_dir)
        self.assertFalse(config.keep_local_addresses)
        self.assertFalse(config.unsafe_logging)
        self.assertEqual(config.max_peers, DEFAULT_SNOWFLAKE_CAPACITY)
        self.assertFalse(config.display_version)

    def test_all_arguments_short_form(self):
        # Note: argparse in config.py uses long form options like --ice, not short like -i
        # This test will focus on long forms.
        pass

    def test_all_arguments_long_form(self):
        args_list = [
            'prog_name',
            '--ice', 'stun:stun1.example.com, stun:stun2.example.com',
            '--url', 'https://broker.example.com',
            '--fronts', 'front1.example.com,front2.example.com',
            '--ampcache', 'https://amp.example.com',
            '--sqsqueue', 'https://sqs.example.com/queue',
            '--sqscreds', 'base64creds',
            '--log', 'client.log',
            '--log-to-state-dir',
            '--keep-local-addresses',
            '--unsafe-logging',
            '--max', '5',
            '--version'
        ]
        with patch('sys.argv', args_list):
            config = parse_args()

        self.assertEqual(config.ice_servers, ['stun:stun1.example.com', 'stun:stun2.example.com'])
        self.assertEqual(config.broker_url, 'https://broker.example.com')
        self.assertEqual(config.front_domains, ['front1.example.com', 'front2.example.com'])
        self.assertEqual(config.ampcache_url, 'https://amp.example.com')
        self.assertEqual(config.sqs_queue_url, 'https://sqs.example.com/queue')
        self.assertEqual(config.sqs_creds_str, 'base64creds')
        self.assertEqual(config.log_file, 'client.log')
        self.assertTrue(config.log_to_state_dir)
        self.assertTrue(config.keep_local_addresses)
        self.assertTrue(config.unsafe_logging)
        self.assertEqual(config.max_peers, 5)
        self.assertTrue(config.display_version)

    def test_legacy_front_domain(self):
        args_list = ['prog_name', '--front', 'legacyfront.example.com']
        with patch('sys.argv', args_list):
            config = parse_args()
        self.assertEqual(config.front_domains, ['legacyfront.example.com'])

    def test_fronts_overrides_legacy_front(self):
        args_list = ['prog_name', '--fronts', 'newfront.example.com', '--front', 'legacyfront.example.com']
        with patch('sys.argv', args_list):
            config = parse_args()
        self.assertEqual(config.front_domains, ['newfront.example.com'])

    def test_ice_server_parsing(self):
        args_list = ['prog_name', '--ice', 'stun:s1.com, stun:s2.com:3478 , stun:s3.com ']
        with patch('sys.argv', args_list):
            config = parse_args()
        self.assertEqual(config.ice_servers, ['stun:s1.com', 'stun:s2.com:3478', 'stun:s3.com'])

    def test_empty_ice_servers(self):
        args_list = ['prog_name', '--ice', '']
        with patch('sys.argv', args_list):
            config = parse_args()
        self.assertEqual(config.ice_servers, [])

    def test_empty_fronts(self):
        args_list = ['prog_name', '--fronts', '']
        with patch('sys.argv', args_list):
            config = parse_args()
        self.assertEqual(config.front_domains, [])


if __name__ == '__main__':
    unittest.main()
