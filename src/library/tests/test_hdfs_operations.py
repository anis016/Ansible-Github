import os
# --------
import sys
dirname, filename = os.path.split(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dirname)
# --------

from hdfs import InsecureClient
# from hdfs.ext.kerberos import KerberosClient
import unittest
import hdfs_operations
from mock import patch

class Singleton(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        cls.hdfs_client = InsecureClient('http://sandbox.hortonworks.com:50070')
        # cls.hdfs_client = KerberosClient('http://sandbox.hortonworks.com:50070')
        cls.hdfs_path = '/tmp/ansible-test-folder/'

        return cls._instance

class TestHDFSAnsible(unittest.TestCase):

    def setUp(self):
        self.hdfs_client = Singleton().hdfs_client
        self.hdfs_path = Singleton().hdfs_path
        self.hdfs_client.delete(hdfs_path=self.hdfs_path, recursive=True)
        self.hdfs_client.makedirs(self.hdfs_path)

    def tearDown(self):
        self.hdfs_client.delete(hdfs_path=self.hdfs_path, recursive=True)

    @patch('hdfs_operations._path_exists', side_effect=hdfs_operations._path_exists)
    @patch('hdfs_operations.AnsibleModule')
    def test_path_exists_success(self, mock_module, mock_path_exists):
        mock_path_exists.return_value = self.hdfs_path
        exists = hdfs_operations._path_exists(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path)
        self.assertEqual(exists, mock_path_exists.return_value)

    @patch('hdfs_operations._path_exists', side_effect=hdfs_operations._path_exists)
    @patch('hdfs_operations.AnsibleModule')
    def test_path_exists_failure(self, mock_module, mock_path_exists):
        mock_path_exists.return_value = False
        not_exists_path = os.path.join(self.hdfs_path, "/not-exists")
        exists = hdfs_operations._path_exists(mock_module, hdfs_client=self.hdfs_client, hdfs_path=not_exists_path)
        self.assertEqual(exists, mock_path_exists.return_value)

    @patch('hdfs_operations.remove', side_effect=hdfs_operations.remove)
    @patch('hdfs_operations.AnsibleModule')
    def test_remove_given_hdfs_path(self, mock_module, mock_remove):

        mock_remove.return_value = True
        results = hdfs_operations.remove(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=False)
        self.assertEqual(results, mock_remove.return_value)

        hdfs_inner_path = os.path.join(self.hdfs_path, "rtest-folder")
        self.hdfs_client.makedirs(hdfs_inner_path)

        mock_remove.return_value = None
        results = hdfs_operations.remove(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=False)
        self.assertEqual(results, mock_remove.return_value)

        mock_remove.return_value = True
        results = hdfs_operations.remove(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=True)
        self.assertEqual(results, True)

    @patch('hdfs_operations.change_owner', side_effect=hdfs_operations.change_owner)
    @patch('hdfs_operations.AnsibleModule')
    def test_change_owner_of_hdfs_path(self, mock_module, mock_change_owner):
        new_owner = "hdfs"

        mock_change_owner.return_value = True
        changed_owner = hdfs_operations.change_owner(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                              owner=new_owner)
        self.assertEqual(changed_owner['changed'], mock_change_owner.return_value)

        mock_change_owner.return_value = False
        changed_owner = hdfs_operations.change_owner(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                              owner=new_owner)
        self.assertEqual(changed_owner['changed'], mock_change_owner.return_value)

    @patch('hdfs_operations.change_group', side_effect=hdfs_operations.change_group)
    @patch('hdfs_operations.AnsibleModule')
    def test_change_group_of_hdfs_path(self, mock_module, mock_change_group):
        new_group = "hdfs"

        mock_change_group.return_value = True
        changed_group = hdfs_operations.change_group(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                              group=new_group)
        self.assertEqual(changed_group['changed'], mock_change_group.return_value)

        mock_change_group.return_value = False
        changed_group = hdfs_operations.change_group(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                              group=new_group)
        self.assertEqual(changed_group['changed'], False)

    @patch('hdfs_operations.change_permission', side_effect=hdfs_operations.change_permission)
    @patch('hdfs_operations.AnsibleModule')
    def test_change_permission_of_hdfs_path(self, mock_module, mock_change_permission):
        new_permission = "0777"

        mock_change_permission.return_value = True
        changed_permission = hdfs_operations.change_permission(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                                        permission=new_permission)
        self.assertEqual(changed_permission['changed'], mock_change_permission.return_value)

        mock_change_permission.return_value = False
        changed_permission = hdfs_operations.change_permission(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                                        permission=new_permission)
        self.assertEqual(changed_permission['changed'], mock_change_permission.return_value)

    @patch('hdfs_operations.upload_localfile', side_effect=hdfs_operations.upload_localfile)
    @patch('hdfs_operations.AnsibleModule')
    def test_upload_localfile_in_hdfs_path(self, mock_module, mock_upload_localfile):
        local_file = "dummy1"

        # create a blank file
        with open(local_file, 'w+'):
            pass

        uploaded = hdfs_operations.upload_localfile(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                 local_path=local_file)
        mock_upload_localfile.return_value = True
        self.assertEqual(uploaded, mock_upload_localfile.return_value)

        # try to upload twice with same content
        mock_upload_localfile.return_value = None
        uploaded = hdfs_operations.upload_localfile(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                 local_path=local_file)
        self.assertEqual(uploaded, mock_upload_localfile.return_value)

        with open(local_file, 'w+') as file:
            file.write("Hello World!")

        # try to upload twice with same file but different content
        mock_upload_localfile.return_value = True
        uploaded = hdfs_operations.upload_localfile(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                    local_path=local_file)
        self.assertEqual(uploaded, mock_upload_localfile.return_value)

        if os.path.exists(local_file):
            os.remove(local_file)

    @patch('hdfs_operations.create_directory', side_effect=hdfs_operations.create_directory)
    @patch('hdfs_operations.AnsibleModule')
    def test_create_new_directory(self, mock_module, mock_create_directory):
        mock_create_directory.return_value = True
        new_hdfs_dir = os.path.join(self.hdfs_path, "new-dir")
        new_dir_success = hdfs_operations.create_directory(mock_module, hdfs_client=self.hdfs_client, hdfs_path=new_hdfs_dir)
        self.assertEqual(new_dir_success, mock_create_directory.return_value)

        mock_create_directory.return_value = False
        new_dir_success = hdfs_operations.create_directory(mock_module, hdfs_client=self.hdfs_client, hdfs_path=new_hdfs_dir)
        self.assertEqual(new_dir_success, mock_create_directory.return_value)