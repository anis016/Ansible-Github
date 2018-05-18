#!/usr/bin/python2.6

ANSIBLE_METADATA = {'metadata_version': '1.0',
                    'status': ['preview'],
                    'supported_by': 'community'}

# set "dfs.namenode.acls.enabled=true" to enable support for ACLs in hdfs-site.xml
from hdfs import InsecureClient
import os
from ansible.module_utils.basic import AnsibleModule

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

def path_exists(hdfs_client, hdfs_path=None):
    if hdfs_path is None:
        raise ValueError("hdfs path should not be empty.")
    try:
        if hdfs_client.status(hdfs_path):
            return hdfs_path
    except:
        print "'{0}': No such file or directory".format(hdfs_path)
        return False

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

def run(module, hdfs_client):
    params = module.params
    command = params['command']
    recurse = params['recurse']
    hdfs_path = params["hdfsPath"]

    if command == "ls":
        module.exit_json(changed=False, hdfs_files=list_files(hdfs_client, hdfs_path=hdfs_path, recursive=recurse))

def main():
    fields = {
        "webhdfs_host": {"required": True, "type": "str"},
        "webhdfs_port": {"required": True, "type": "str"},
        "effective_user": {"required": True, "default": None, "type": "str"},
        "recurse": {"default": False, "type": "bool"},
        "command": {"default": None, "choices": ['ls']},
        "hdfsPath": {"required": True, "type": "str"},
        "localPath": {"required": False, "type": "str"},
    }

    try:
        module = AnsibleModule(argument_spec=fields)
        params = module.params
        web_hdfs_url = params["webhdfs_host"] + ":" + params["webhdfs_port"]
        user = params["effective_user"]

        hdfs_client = InsecureClient(web_hdfs_url, user=user)
        run(module, hdfs_client)

    except Exception as e:
        module.fail_json(msg='Unable to init WEB HDFS client for %s:%s: %s' % (
            params['webhdfs_port'], params['effective_user'], str(e)))

if __name__ == '__main__':
    main()