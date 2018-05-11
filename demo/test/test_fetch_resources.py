#!/usr/bin/env python

import unittest
from ansible.compat.tests.mock import call, create_autospec, patch, mock_open
from ansible.module_utils.basic import AnsibleModule
from ansible.demo.libray import fetch_resources

class AddTester(unittest.TestCase):

    def setUp(self):
        self.a = 10
        self.b = 23

    # this function will
    def test_add(self):
      c = 33
      assert self.a + self.b == c

   # this function will
    def test_subtract(self):
      c = -13
      assert self.a - self.b == c