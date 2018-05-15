#!/usr/bin/env python

# run command:
# nosetests --doctest-tests -v demo/library/test_fetch_resources.py

import unittest
from ansible.compat.tests.mock import call, create_autospec, patch, mock_open
from ansible.module_utils.basic import AnsibleModule
import fetch_resources as fetch_resources

class TestFetchResource(unittest.TestCase):

    @patch('fetch_resources.write')
    @patch('fetch_resources.fetch')
    def test_save_data(self, fetch, write):
        # Setup
        mod_cls = create_autospec(AnsibleModule)
        mod = mod_cls.return_value
        mod.params = dict(
            url="https://www.google.com",
            dest="/tmp/testAnsible.txt"
        )

        # Exercise
        fetch_resources.save_data(mod)

        # Verify
        self.assertEqual(1, fetch.call_count)
        expected = call(mod.params["url"])
        self.assertEqual(expected, fetch.call_args)

        self.assertEqual(1, write.call_count)
        expected = call(fetch.return_value, mod.params["dest"])
        self.assertEqual(expected, write.call_args)

        self.assertEqual(1, mod.exit_json.call_count)
        expected = call(msg="Data saved", changed=True)
        self.assertEqual(expected, mod.exit_json.call_args)

    @patch('fetch_resources.open_url')
    def test_fetch(self, open_url):
        # setup
        url = "https://www.google.com"

        # mock the return value of open_url
        stream = open_url.return_value
        stream.read.return_value = "<html><head></head><body>Hello</body></html>"
        stream.getcode.return_value = 200
        open_url.return_value = stream

        # exercise
        data = fetch_resources.fetch(url)

        # verify
        self.assertEqual(stream.read.return_value, data)
        self.assertEqual(1, open_url.call_count)
        expected = call(url)
        self.assertEqual(expected, open_url.call_args)

    def test_write(self):
        # setup
        data = "Somedata"
        dest = "/tmp/testAnsible.txt"

        # exercise
        obj_open = "fetch_resources.open"
        mok_open = mock_open()
        with patch(obj_open, mok_open, create=True):
            fetch_resources.write(data=data, dest=dest)

        # verify
        expected = call(dest, "w")
        self.assertEqual(expected, mok_open.mock_calls[0])
        expected = call().write(data)
        self.assertEqual(expected, mok_open.mock_calls[2])