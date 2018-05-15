#!/usr/bin/python

# set "dfs.namenode.acls.enabled=true" to enable support for ACLs in hdfs-site.xml
from hdfs import InsecureClient, HdfsError
import os
from ansible.module_utils.basic import AnsibleModule

def print_something(mod):
    print mod

### listing ###
def list_files(hdfs_client, hdfs_path=None, recursive=False):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        if recursive is False:
            return hdfs_client.list(hdfs_path)
        else:
            files, folder = [], []
            folder.append(hdfs_path)
            while len(folder) > 0:
                hdfs_path = folder.pop()
                file_lists = hdfs_client.list(hdfs_path)
                for file in file_lists:
                    file_path = os.path.join(hdfs_path + "/", file)
                    if hdfs_client.status(file_path)["type"] == "DIRECTORY":
                        folder.append(file_path)
                        files.append(file_path)
                    else:
                        files.append(file_path)

            return files

### deleting ###
def delete(hdfs_client, hdfs_path=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    if path_exists(hdfs_client, hdfs_path):
        success = hdfs_client.delete(hdfs_path, recursive=True)
        if success:
            print "Deleted {0}".format(hdfs_path)
            return success
    else:
        return False

def path_exists(hdfs_client, hdfs_path=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    try:
        if hdfs_client.status(hdfs_path):
            return hdfs_path
    except:
        print "'{0}': No such file or directory".format(hdfs_path)
        return False

### change the owner ###
def change_owner(hdfs_client, hdfs_path=None, owner=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    if owner is None:
        raise ValueError("owner should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        current_owner = hdfs_client.status(hdfs_path)["owner"]
        # print current_owner
        hdfs_client.set_owner(hdfs_path, owner=owner)
        new_owner = hdfs_client.status(hdfs_path)["owner"]
        print "Owner Changed: {0} ".format(current_owner != new_owner)
        return (current_owner != new_owner)

### change the group ###
def change_group(hdfs_client, hdfs_path=None, group=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    if group is None:
        raise ValueError("group should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        current_group = hdfs_client.status(hdfs_path)["group"]
        # print current_group
        hdfs_client.set_owner(hdfs_path, group=group)
        new_group = hdfs_client.status(hdfs_path)["group"]
        print "Group Changed: {0} ".format(current_group != new_group)
        return (current_group != new_group)

### change the permission ###
def change_permission(hdfs_client, hdfs_path=None, permission=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    if permission is None:
        raise ValueError("permission should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        current_permission = hdfs_client.status(hdfs_path)["permission"]
        # print current_permission
        hdfs_client.set_permission(hdfs_path, permission=permission)
        new_permission = hdfs_client.acl_status(hdfs_path)["permission"]
        print "Permission Changed: {0} ".format(current_permission != new_permission)
        return (current_permission != new_permission)

### upload file ###
def upload_localfile(hdfs_client, hdfs_path=None, local_path=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    if local_path is None:
        raise ValueError("local path should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        file_name = local_path.split("/")[-1]
        files_list = hdfs_client.list(hdfs_path)
        if file_name not in files_list:
            uploaded_path = hdfs_client.upload(hdfs_path=hdfs_path, local_path=local_path)
            status = "uploaded: {0} ".format(uploaded_path)
        else:
            status = "file already exists."

        return status

### hdfs path status  ###
def status(hdfs_client, hdfs_path):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        hdfs_status = hdfs_client.status(hdfs_path)
        return hdfs_status

### creating directory ###
def make_dirs(hdfs_client, hdfs_path):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")

    # remove "/" at end, if present
    hdfs_path = hdfs_path[:len(hdfs_path)-1] if hdfs_path.endswith("/") else hdfs_path
    # print hdfs_path

    # collect dir_name and parent directory path from the hdfs path
    dir_name = hdfs_path.split("/")[-1]
    parent_dir = "/".join(hdfs_path.split("/")[:-1])
    # print dir_name
    # print parent_dir

    # list all the files
    files_list = hdfs_client.list(parent_dir)
    if dir_name in files_list:
        print "directory exists."
    else:
        hdfs_client.makedirs(hdfs_path)
        print "new directory created."

if __name__ == '__main__':
    pass

    # module = AnsibleModule(
    #     argument_spec=dict(
    #         namenode_host=dict(required=True, type='str'),
    #         namenode_port=dict(required=False, default=8020, type='int'),
    #         effective_user=dict(required=False, default=None, type='str'),
    #         state=dict(choices=['file', 'directory', 'touchz', 'absent'], default=None),
    #         path=dict(aliases=['dest', 'name'], required=True, type='path'),
    #         mode=dict(required=False, default=None, type='raw'),
    #         owner=dict(required=False, default=None, type='str'),
    #         group=dict(required=False, default=None, type='str'),
    #         original_basename=dict(required=False),  # Internal use only, for recursive ops
    #         recurse=dict(default=False, type='bool'),
    #         diff_peek=dict(default=None),  # Internal use only, for internal checks in the action plugins
    #         validate=dict(required=False, default=None),  # Internal use only, for template and copy
    #         src=dict(required=False, default=None, type='path'),
    #     ),
    #     supports_check_mode=True
    # )

    # hdfs_client = InsecureClient('http://sandbox.hortonworks.com:50070', user='sayed')
    # hdfs_path = "/tmp/dqjob"
    # local_path = "/home/sayed/kernel_cleaner.sh"

    # files = list_files(hdfs_client, hdfs_path, recursive=True)
    # print files

    # delete(hdfs_client, hdfs_path)

    # make_dirs(hdfs_client, hdfs_path)

