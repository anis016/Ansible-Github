#!/usr/bin/python2.6

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: my_sample_module

short_description: This is my sample module

version_added: "2.4"

description:
    - "This is my longer description explaining my sample module"

options:
    name:
        description:
            - This is the message to send to the sample module
        required: true
    new:
        description:
            - Control to demo if the result of this module is changed or not
        required: false

extends_documentation_fragment:
    - azure

author:
    - Your Name (@yourhandle)
'''

EXAMPLES = '''
# Pass in a message
- name: Test with a message
  my_new_test_module:
    name: hello world

# pass in a message and have changed true
- name: Test with a message and changed output
  my_new_test_module:
    name: hello world
    new: true

# fail the module
- name: Test failure of the module
  my_new_test_module:
    name: fail me
'''

RETURN = '''
original_message:
    description: The original name param that was passed in
    type: str
message:
    description: The output message that the sample module generates
'''
# set "dfs.namenode.acls.enabled=true" to enable support for ACLs in hdfs-site.xml
import os
from ansible.module_utils.basic import AnsibleModule
from hdfs import InsecureClient
from hdfs import HdfsError
# from hdfs.ext.kerberos import KerberosClient

### listing ###
def list_files(module, hdfs_client, hdfs_path=None, recursive=False):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
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

def path_exists(module, hdfs_client, hdfs_path=None):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    try:
        if hdfs_client.status(hdfs_path):
            return hdfs_path
    except:
        return False

### deleting ###
def delete(module, hdfs_client, hdfs_path=None, recursive=False):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        try:
            success = hdfs_client.delete(hdfs_path, recursive=recursive)
            if success is True:
                return True
        except HdfsError:
            return False

    else:
        return False

### change the owner ###
def change_owner(module, hdfs_client, hdfs_path=None, owner=None):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if owner is None:
        module.fail_json(msg="owner should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        current_owner = hdfs_client.status(hdfs_path)["owner"]
        hdfs_client.set_owner(hdfs_path, owner=owner)
        new_owner = hdfs_client.status(hdfs_path)["owner"]
        return (current_owner != new_owner)

### change the group ###
def change_group(module, hdfs_client, hdfs_path=None, group=None):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if group is None:
        module.fail_json(msg="group should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        current_group = hdfs_client.status(hdfs_path)["group"]
        hdfs_client.set_owner(hdfs_path, group=group)
        new_group = hdfs_client.status(hdfs_path)["group"]
        return (current_group != new_group)

### change the permission ###
def change_permission(module, hdfs_client, hdfs_path=None, permission=None):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if permission is None:
        module.fail_json(msg="permission should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        current_permission = hdfs_client.status(hdfs_path)["permission"]
        hdfs_client.set_permission(hdfs_path, permission=permission)
        new_permission = hdfs_client.acl_status(hdfs_path)["permission"]
        return (current_permission != new_permission)

### upload file ###
def upload_localfile(module, hdfs_client, hdfs_path=None, local_path=None):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if local_path is None:
        module.fail_json(msg="local path should not be empty.")

    if path_exists(hdfs_client, hdfs_path):
        file_name = local_path.split("/")[-1]
        files_list = hdfs_client.list(hdfs_path)
        if file_name not in files_list:
            uploaded_path = hdfs_client.upload(hdfs_path=hdfs_path, local_path=local_path)
            status = uploaded_path
        else:
            status = False

        return status

### hdfs path status  ###
def status(module, hdfs_client, hdfs_path):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        hdfs_status = hdfs_client.status(hdfs_path)
        return hdfs_status

### creating directory ###
def make_directory(module, hdfs_client, hdfs_path):
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    # remove "/" at end, if present
    hdfs_path = hdfs_path[:len(hdfs_path)-1] if hdfs_path.endswith("/") else hdfs_path

    # collect dir_name and parent directory path from the hdfs path
    dir_name = hdfs_path.split("/")[-1]
    parent_dir = "/".join(hdfs_path.split("/")[:-1])

    # list all the files
    files_list = hdfs_client.list(parent_dir)
    if dir_name in files_list:
        return False
    else:
        hdfs_client.makedirs(hdfs_path)
        return "new directory created."

def run(module, hdfs_client):
    params = module.params
    command = params['command']
    recurse = params['recurse']
    hdfs_path = params["hdfsPath"]
    owner = params["owner"]
    group = params["group"]
    permission = params["permission"]
    local_path = params["localPath"]

    if command == "ls":
        hdfs_files = list_files(module, hdfs_client, hdfs_path=hdfs_path, recursive=recurse)
        module.exit_json(changed=False, msg=hdfs_files)
    elif command == "exists":
        path = path_exists(module, hdfs_client, hdfs_path=hdfs_path)
        if path is False:
            module.exit_json(changed=False, msg="{0} - No such file or directory".format(hdfs_path))
        else:
            module.exit_json(changed=False, msg="{0}".format(hdfs_path))
    elif command == "delete":
        result = delete(module, hdfs_client, hdfs_path=hdfs_path, recursive=recurse)
        if result == False:
            module.exit_json(changed=False, msg="{0} - No such file or directory".format(hdfs_path))
        else:
            module.exit_json(changed=True, msg="Deleted {0}".format(hdfs_path))
    elif command == "owner":
        result = change_owner(module, hdfs_client, hdfs_path=hdfs_path, owner=owner)
        if result == False:
            module.exit_json(changed=False, msg="{0}".format(status(hdfs_client, hdfs_path)))
        else:
            module.exit_json(changed=True, msg="Owner Changed {0}".format(hdfs_path))
    elif command == "group":
        result = change_group(module, hdfs_client, hdfs_path=hdfs_path, group=group)
        if result == False:
            module.exit_json(changed=False, msg="{0}".format(status(hdfs_client, hdfs_path)))
        else:
            module.exit_json(changed=True, msg="Group Changed {0}".format(hdfs_path))
    elif command == "permission":
        result = change_permission(module, hdfs_client, hdfs_path=hdfs_path, permission=permission)
        if result == False:
            module.exit_json(changed=False, msg="{0}".format(status(hdfs_client, hdfs_path)))
        else:
            module.exit_json(changed=True, msg="Permission Changed {0}".format(hdfs_path))
    elif command == "put":
        uploaded = upload_localfile(module, hdfs_client, hdfs_path=hdfs_path, local_path=local_path)
        if uploaded == False:
            module.exit_json(changed=False, msg="{0} file already exists".format(local_path))
        else:
            module.exit_json(changed=True, msg="uploaded: {0} ".format(uploaded))
    elif command == "makedir":
        created_dir = make_directory(module, hdfs_client, hdfs_path)
        if created_dir == False:
            module.exit_json(changed=False, msg="{0} directory exists.".format(local_path))
        else:
            module.exit_json(changed=True, msg="{0} ".format(created_dir))

def main():
    # hdfs_client = InsecureClient("http://127.0.0.1:50070", user="sayed")
    # hdfs_path = "/tmp/ansible"
    # print list_files(hdfs_client, hdfs_path=hdfs_path)

    fields = {
        "webhdfs_host": {"required": True, "type": "str"},
        "webhdfs_port": {"required": True, "type": "str"},
        "user": {"required": True, "default": None, "type": "str"},
        "recurse": {"default": False, "type": "bool"},
        "command": {"default": None, "choices": ["ls", "exists", "delete", "owner", "group", "permission", "put", "makedir"]},
        "hdfsPath": {"required": True, "type": "str"},
        "localPath": {"required": False, "type": "str"},
        "owner": {"required": False, "type": "str"},
        "group": {"required": False, "type": "str"},
        "permission": {"required": False, "type": "str"},
    }

    module = AnsibleModule(argument_spec=fields, supports_check_mode=True)

    try:
        params = module.params
        web_hdfs_url = params["webhdfs_host"] + ":" + params["webhdfs_port"]
        user = params["effective_user"]

        hdfs_client = InsecureClient(web_hdfs_url, user=user)
        # hdfs_client = KerberosClient(web_hdfs_url)
        run(module, hdfs_client)

    except Exception as e:
        module.fail_json(msg='Unable to init WEB HDFS client for %s:%s: %s' % (
            params['webhdfs_port'], params['user'], str(e)))

if __name__ == '__main__':
    main()