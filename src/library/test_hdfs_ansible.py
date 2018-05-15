
import unittest
from ansible.compat.tests.mock import call, create_autospec, patch, mock_open
from hdfs import InsecureClient
from ansible.module_utils.basic import AnsibleModule
import hdfs_ansible as hdfs_ansible

class Singleton(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        cls.hdfs_client = InsecureClient('http://sandbox.hortonworks.com:50070', user='sayed')
        cls.hdfs_path = '/tmp/'
        return cls._instance

class TestHDFSAnsible(unittest.TestCase):

    def setUp(self):
        self.hdfs_client = Singleton().hdfs_client
        self.hdfs_path = Singleton().hdfs_path

    @patch('hdfs_ansible.list_files')
    def test_list_files(self, list_files):
        mod_cls = create_autospec(AnsibleModule)
        mod = mod_cls.return_value
        mod.params = dict(
            hdfs_client=self.hdfs_client,
            hdfs_path=self.hdfs_path
        )

        # Exercise
        hdfs_ansible.list_files(hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=False)

    @patch('hdfs_ansible.path_exists')
    def test_path_exists(self, path_exists):
        print path_exists

