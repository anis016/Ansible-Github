
from hdfs import InsecureClient
# from hdfs.ext.kerberos import KerberosClient
import unittest
import hdfs_ansible as hdfs_ansible
from mock import patch
import os

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

    @patch('hdfs_ansible.list_files', side_effect=hdfs_ansible.list_files)
    @patch('hdfs_ansible.AnsibleModule')
    def test_list_hdfs_files(self, mock_module, mock_list_files):

        for path in ("dummy1", "dummy2", "dummy3"):
            self.hdfs_client.upload(hdfs_path=self.hdfs_path, local_path=path)
        mock_list_files.return_value = 3

        files = hdfs_ansible.list_files(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=False)
        self.assertEqual(len(files), mock_list_files.return_value)

        inner_path = os.path.join(self.hdfs_path, "rtest-folder")
        self.hdfs_client.makedirs(inner_path)
        self.hdfs_client.upload(hdfs_path=inner_path, local_path="dummy4")
        mock_list_files.return_value = 5

        files = hdfs_ansible.list_files(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=True)
        self.assertEqual(len(files), mock_list_files.return_value)

    @patch('hdfs_ansible.path_exists', side_effect=hdfs_ansible.path_exists)
    @patch('hdfs_ansible.AnsibleModule')
    def test_path_exists_success(self, mock_module, mock_path_exists):
        mock_path_exists.return_value = self.hdfs_path
        exists = hdfs_ansible.path_exists(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path)
        self.assertEqual(exists, mock_path_exists.return_value)

    @patch('hdfs_ansible.path_exists', side_effect=hdfs_ansible.path_exists)
    @patch('hdfs_ansible.AnsibleModule')
    def test_path_exists_failure(self, mock_module, mock_path_exists):
        mock_path_exists.return_value = False
        not_exists_path = os.path.join(self.hdfs_path, "/not-exists")
        exists = hdfs_ansible.path_exists(mock_module, hdfs_client=self.hdfs_client, hdfs_path=not_exists_path)
        self.assertEqual(exists, mock_path_exists.return_value)

    @patch('hdfs_ansible.remove', side_effect=hdfs_ansible.remove)
    @patch('hdfs_ansible.AnsibleModule')
    def test_delete_given_hdfs_path(self, mock_module, mock_remove):

        mock_remove.return_value = True
        results = hdfs_ansible.remove(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=False)
        self.assertEqual(results, mock_remove.return_value)

        hdfs_inner_path = os.path.join(self.hdfs_path, "rtest-folder")
        self.hdfs_client.makedirs(hdfs_inner_path)

        mock_remove.return_value = False
        results = hdfs_ansible.remove(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=False)
        self.assertEqual(results, mock_remove.return_value)

        mock_remove.return_value = True
        results = hdfs_ansible.remove(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path, recursive=True)
        self.assertEqual(results, True)

    @patch('hdfs_ansible.change_owner', side_effect=hdfs_ansible.change_owner)
    @patch('hdfs_ansible.AnsibleModule')
    def test_change_owner_of_hdfs_path(self, mock_module, mock_change_owner):
        new_owner = "hdfs"

        mock_change_owner.return_value = True
        changed_owner = hdfs_ansible.change_owner(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                  owner=new_owner)
        self.assertEqual(changed_owner, mock_change_owner.return_value)

        mock_change_owner.return_value = False
        changed_owner = hdfs_ansible.change_owner(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                  owner=new_owner)
        self.assertEqual(changed_owner, mock_change_owner.return_value)

    @patch('hdfs_ansible.change_group', side_effect=hdfs_ansible.change_group)
    @patch('hdfs_ansible.AnsibleModule')
    def test_change_group(self, mock_module, mock_change_group):
        new_group = "hdfs"

        mock_change_group.return_value = True
        changed_group = hdfs_ansible.change_group(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                  group=new_group)
        self.assertEqual(changed_group, mock_change_group.return_value)

        mock_change_group.return_value = False
        changed_group = hdfs_ansible.change_group(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                  group=new_group)
        self.assertEqual(changed_group, False)

    @patch('hdfs_ansible.change_permission', side_effect=hdfs_ansible.change_permission)
    @patch('hdfs_ansible.AnsibleModule')
    def test_change_permission(self, mock_module, mock_change_permission):
        new_permission = "0777"

        mock_change_permission.return_value = True
        changed_permission = hdfs_ansible.change_permission(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                            permission=new_permission)
        self.assertEqual(changed_permission, mock_change_permission.return_value)

        mock_change_permission.return_value = False
        changed_permission = hdfs_ansible.change_permission(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                            permission=new_permission)
        self.assertEqual(changed_permission, mock_change_permission.return_value)

    @patch('hdfs_ansible.upload_localfile', side_effect=hdfs_ansible.upload_localfile)
    @patch('hdfs_ansible.AnsibleModule')
    def test_upload_localfile_to_hdfs_path(self, mock_module, mock_upload_localfile):
        mock_upload_localfile.return_value = os.path.join(self.hdfs_path, "dummy1")
        uploaded = hdfs_ansible.upload_localfile(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                 local_path="dummy1")
        self.assertEqual(uploaded, mock_upload_localfile.return_value)

        mock_upload_localfile.return_value = False
        uploaded = hdfs_ansible.upload_localfile(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                 local_path="dummy1")
        self.assertEqual(uploaded, mock_upload_localfile.return_value)

    @patch('hdfs_ansible.make_directory', side_effect=hdfs_ansible.make_directory)
    @patch('hdfs_ansible.AnsibleModule')
    def test_make_new_directory(self, mock_module, mock_make_directory):
        mock_make_directory.return_value = "new directory created."
        new_hdfs_dir = os.path.join(self.hdfs_path, "new-dir")
        new_dir_success = hdfs_ansible.make_directory(mock_module, hdfs_client=self.hdfs_client, hdfs_path=new_hdfs_dir)
        self.assertEqual(new_dir_success, mock_make_directory.return_value)

        mock_make_directory.return_value = False
        uploaded = hdfs_ansible.upload_localfile(mock_module, hdfs_client=self.hdfs_client, hdfs_path=self.hdfs_path,
                                                 local_path="dummy1")
        new_dir_success = hdfs_ansible.make_directory(mock_module, hdfs_client=self.hdfs_client, hdfs_path=uploaded)
        self.assertEqual(new_dir_success, mock_make_directory.return_value)