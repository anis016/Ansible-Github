#!/usr/bin/python

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: hdfs_ansible
short_description: Performs operations against HDFS.
description:
    Given a hdfs path, this module performs the below operations.
    - Uploading a local file to HDFS (hdfs dfs -put ...)
    - Change the permissions of a file or directory (hdfs dfs -chmod ...)
    - Change the owner of a file or directory (hdfs dfs -chown ...)
    - Change the group of a file or directory (hdfs dfs -chown ...)
    - Remove a file or directory in HDFS (hdfs dfs -rm ...)
    - List a directory in HDFS (hdfs dfs -ls ...)
    - Make a new directory in HDFS (hdfs dfs -mkdir ...)
    - Checks if a file or directory exists in HDFS.
    This modules uses the HTTP REST API for interfacing with HDFS and it's all the mentioned operations.
version_added: "2.4"
requirements: [ "hdfs (Python 2.X WebHDFS client)",
                "requests-kerberos (Kerberos requests)",
                "pykerberos (A high-level wrapper for Kerberos (GSSAPI) operations)" ]
options:        
    webhdfs_http_url:
        description:
            - WebHDFS URL with Hostname and PORT.
        required: True
    hdfsPath:
        description:
            - HDFS Path on which the operations will be carried out.
        required: True
    command:
        description:
            - Commands that performs certain operations. Please check the description for what each command does.
        required: True
    localPath:
        description:
            - Local file path. This is required for "put" command that will upload files into the certain HDFS directory.
        required: False
    recurse:
        description:
            - Recursively visits the directory.
        required: False
    owner:
        description:
            - Changes the owner of a file or directory.
        required: False
    group:
        description:
            - Changes the group of a file or directory.
        required: False
    permission:
        description:
            - Changes the permission(octal) eg: 0777 of a file or directory.
        required: False
author:
    - Sayed Anisul Hoque @ UT
'''

EXAMPLES = '''
# list all the files in the hdfs path
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/
    recurse: False
    command: ls
  
# check if the hdfs path exists
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/not-exist
    command: exists
  
# removes file or directory if the hdfs path exists
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/not-exist
    command: rm
  
# sets owner of the hdfs file or directory
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/kernel_cleaner.sh
    command: chown
    owner: solr
  
# sets group of the hdfs file or directory
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/kernel_cleaner.sh
    command: chgrp
    group: solr

# sets permissions of the hdfs file or directory
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/kernel_cleaner.sh
    command: chmod
    permission: "0666"

# creates new directory in hdfs if not exists
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp/some-new-folder
    command: mkdir

# uploads a local file to hdfs
- hdfs_ansible:
    webhdfs_http_url: http://sandbox.hortonworks.com:50070
    hdfsPath: /tmp
    localPath: /home/sayed/oozie-document-sla-retrieval.adoc
    command: put
'''

RETURN = '''
msg:
    description: The output message that the sample module generates.
'''

import os
from ansible.module_utils.basic import AnsibleModule
from hdfs import InsecureClient
from hdfs import HdfsError
# from hdfs.ext.kerberos import KerberosClient

def list_files(module, hdfs_client, hdfs_path=None, recursive=False):
    """
    List all the files in the hdfs path.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :param recursive: if recursive is set to True then recursively lists all subdirectories.
    :return: list of all the files in the given hdfs path.
    """
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
    """
    Checks if the hdfs path exists.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :return: returns hdfs path if path exists, False otherwise.
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    try:
        if hdfs_client.status(hdfs_path):
            return hdfs_path
    except:
        return False

def remove(module, hdfs_client, hdfs_path=None, recursive=False):
    """
    Removes file or directory if the hdfs path exists.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :param recursive: if recursive is set to True then recursively delete's subdirectories.
    :return: True if success, False otherwise.
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        try:
            success = hdfs_client.delete(hdfs_path, recursive=recursive)
            if success is True:
                return True
        except HdfsError:
            return False
    else:
        return False

def change_owner(module, hdfs_client, hdfs_path=None, owner=None):
    """
    Sets owner of the hdfs file or directory.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :param owner: owner
    :return: True if owner is changed, False otherwise.
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if owner is None:
        module.fail_json(msg="owner should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        current_owner = hdfs_client.status(hdfs_path)["owner"]
        hdfs_client.set_owner(hdfs_path, owner=owner)
        new_owner = hdfs_client.status(hdfs_path)["owner"]
        return (current_owner != new_owner)

def change_group(module, hdfs_client, hdfs_path=None, group=None):
    """
    Sets group of the hdfs file or directory.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :param group: group
    :return: True if group is changed, False otherwise.
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if group is None:
        module.fail_json(msg="group should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        current_group = hdfs_client.status(hdfs_path)["group"]
        hdfs_client.set_owner(hdfs_path, group=group)
        new_group = hdfs_client.status(hdfs_path)["group"]
        return (current_group != new_group)

def change_permission(module, hdfs_client, hdfs_path=None, permission=None):
    """
    Sets permissions of the hdfs file or directory.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :param permission: permission (in octal string)
    :return: True if permission is changed, False otherwise.
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if permission is None:
        module.fail_json(msg="permission should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        current_permission = hdfs_client.status(hdfs_path)["permission"]
        hdfs_client.set_permission(hdfs_path, permission=permission)
        new_permission = hdfs_client.acl_status(hdfs_path)["permission"]
        return (current_permission != new_permission)

def upload_localfile(module, hdfs_client, hdfs_path=None, local_path=None):
    """
    Uploads a local file into hdfs directory.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :param local_path: local file path
    :return: uploaded hdfs path if operation is success, else False.
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")
    if local_path is None:
        module.fail_json(msg="local path should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        file_name = local_path.split("/")[-1]
        files_list = hdfs_client.list(hdfs_path)
        if file_name not in files_list:
            uploaded_path = hdfs_client.upload(hdfs_path=hdfs_path, local_path=local_path)
            status = uploaded_path
        else:
            status = False

        return status

def status(module, hdfs_client, hdfs_path):
    """
    Information of the hdfs path.
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :return: status of the hdfs path
    """
    if hdfs_path is None:
        module.fail_json(msg="hdfs path should not be empty.")

    if path_exists(module, hdfs_client, hdfs_path):
        hdfs_status = hdfs_client.status(hdfs_path)
        return hdfs_status

def make_directory(module, hdfs_client, hdfs_path):
    """
    Creates a new file or directory in hdfs if not exists
    :param module: ansible module
    :param hdfs_client: hdfs client
    :param hdfs_path: hdfs path
    :return: False if directory already exists, newly created hdfs path otherwise.
    """
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
    """
    Run's the hdfs ansible module. Performs operations with the given HDFS command.
    :param module: ansible module
    :param hdfs_client: hdfs client
    """
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
    elif command == "rm":
        result = remove(module, hdfs_client, hdfs_path=hdfs_path, recursive=recurse)
        if result == False:
            module.exit_json(changed=False, msg="{0} - No such file or directory".format(hdfs_path))
        else:
            module.exit_json(changed=True, msg="Deleted {0}".format(hdfs_path))
    elif command == "chown":
        result = change_owner(module, hdfs_client, hdfs_path=hdfs_path, owner=owner)
        if result == False:
            module.exit_json(changed=False, msg="{0}".format(status(module, hdfs_client, hdfs_path)))
        else:
            module.exit_json(changed=True, msg="Owner Changed {0}".format(hdfs_path))
    elif command == "chgrp":
        result = change_group(module, hdfs_client, hdfs_path=hdfs_path, group=group)
        if result == False:
            module.exit_json(changed=False, msg="{0}".format(status(module, hdfs_client, hdfs_path)))
        else:
            module.exit_json(changed=True, msg="Group Changed {0}".format(hdfs_path))
    elif command == "chmod":
        result = change_permission(module, hdfs_client, hdfs_path=hdfs_path, permission=permission)
        if result == False:
            module.exit_json(changed=False, msg="{0}".format(status(module, hdfs_client, hdfs_path)))
        else:
            module.exit_json(changed=True, msg="Permission Changed {0}".format(hdfs_path))
    elif command == "put":
        uploaded = upload_localfile(module, hdfs_client, hdfs_path=hdfs_path, local_path=local_path)
        if uploaded == False:
            module.exit_json(changed=False, msg="{0} file already exists".format(local_path))
        else:
            module.exit_json(changed=True, msg="uploaded: {0} ".format(uploaded))
    elif command == "mkdir":
        created_dir = make_directory(module, hdfs_client, hdfs_path)
        if created_dir == False:
            module.exit_json(changed=False, msg="{0} directory exists.".format(local_path))
        else:
            module.exit_json(changed=True, msg="{0} ".format(created_dir))

def main():
    """
    main entry point of the execution.
    """
    fields = {
        "webhdfs_http_url": {"required": True, "type": "str"},
        "hdfsPath": {"required": True, "type": "str"},
        "command": {"required": True,
                    "choices": ["ls", "exists", "rm", "chown", "chgrp", "chmod", "put", "mkdir"],
                    "type": "str"},
        "recurse": {"default": False, "type": "bool"},
        "localPath": {"required": False, "type": "str"},
        "owner": {"required": False, "type": "str"},
        "group": {"required": False, "type": "str"},
        "permission": {"required": False, "type": "str"},
    }

    module = AnsibleModule(argument_spec=fields, supports_check_mode=True)

    try:
        params = module.params
        webhdfs_url = params["webhdfs_http_url"]

        hdfs_client = InsecureClient(webhdfs_url)
        # hdfs_client = KerberosClient(webhdfs_url)
        run(module, hdfs_client)

    except Exception as e:
        module.fail_json(msg='Unable to init WEB HDFS client for %s: %s' % (
            params['webhdfs_http_url'], str(e)))

if __name__ == '__main__':
    main()