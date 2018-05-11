#!/usr/bin/env python
import sys
sys.path.append("..")

import unittest
from ansible.compat.tests.mock import call, create_autospec, patch, mock_open
from ansible.module_utils.basic import AnsibleModule
from library import fetch_resources


# class TestFetchResource(unittest.TestCase):
#
#     @patch('fetch_resources.write')
#     @patch('fetch_resources.fetch')
#     def test_save_data_happy_path(self, fetch, write):
#         # Setup
#         mod_cls = create_autospec(AnsibleModule)
#         mod = mod_cls.return_value
#         mod.params = dict(
#             url="https://www.google.com",
#             dest="/tmp/firstmod.txt"
#         )
#
#         # Exercise
#         fetch_resources.save_data(mod)
#
#         # Verify
#         self.assertEqual(1, fetch.call_count)
#         expected = call(mod.params["url"])
#         self.assertEqual(expected, fetch.call_args)
#
#         self.assertEqual(1, write.call_count)
#         expected = call(fetch.return_value, mod.params["dest"])
#         self.assertEqual(expected, write.call_args)
#
#         self.assertEqual(1, mod.exit_json.call_count)
#         expected = call(msg="Data saved", changed=True)
#         self.assertEqual(expected, mod.exit_json.call_args)

# run command:
# nosetests --doctest-tests -v demo/test/test_fetch_resources.py