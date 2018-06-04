#!/usr/bin/python

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: hdfs_operations
short_description: Performs operations against HDFS.
description:
    Given a HDFS path, this module performs the below operations.
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
    webhdfs_url:
        description:
            - WebHDFS URL with Hostname and PORT.
        required: True
    hdfs_path:
        description:
            - HDFS Path on which the operations will be carried out.
        required: True
    command:
        description:
            - Commands that performs certain operations. Please check the description for what each command does.
        required: True
    local_path:
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
    - Sayed Anisul Hoque @ Ultra Tendency GmbH
'''

EXAMPLES = '''
# Removes a file or a directory if the HDFS path exists.
- hdfs_operations:
    webhdfs_url: http://sandbox.hortonworks.com:50070
    hdfs_path: /tmp/not-exist
    command: rm
  
# Sets the owner of an HDFS file or directory.
- hdfs_operations:
    webhdfs_url: http://sandbox.hortonworks.com:50070
    hdfs_path: /tmp/kernel_cleaner.sh
    command: chown
    owner: solr
  
# Sets the group of an HDFS file or directory.
- hdfs_operations:
    webhdfs_url: http://sandbox.hortonworks.com:50070
    hdfs_path: /tmp/kernel_cleaner.sh
    command: chgrp
    group: solr

# Sets the permissions of an HDFS file or directory.
- hdfs_operations:
    webhdfs_url: http://sandbox.hortonworks.com:50070
    hdfs_path: /tmp/kernel_cleaner.sh
    command: chmod
    permission: "0666"

# Creates a new directory in the HDFS, if it does not exist.
- hdfs_operations:
    webhdfs_url: http://sandbox.hortonworks.com:50070
    hdfs_path: /tmp/some-new-folder
    command: mkdir

# Uploads a local file in the HDFS directory.
- hdfs_operations:
    webhdfs_url: http://sandbox.hortonworks.com:50070
    hdfs_path: /tmp
    local_path: /home/sayed/oozie-document-sla-retrieval.adoc
    command: put
'''

RETURN = '''
changed:
    description: Determines whether or not this module made any modifications.
    returned: true or false
    type: string
    sample: "true"
path:
    description: Destination HDFS path.
    type: string
    sample: "/tmp"
local_path:
    description: Source file used for the copy on the target HDFS path.
    returned: when supported
    type: string
    sample: "/home/sayed/ansible-tmp-1423796390.97-147729857856000"
msg:
    description: The output message that the different functionalities this module generates.
    type: string
    sample: "uploaded: /tmp/oozie-document-sla-retrieval.adoc"
'''

import hashlib
import os
from ansible.module_utils.basic import AnsibleModule
from hdfs import InsecureClient
from hdfs import HdfsError
# from hdfs.ext.kerberos import KerberosClient

def _path_exists(module, hdfs_client, hdfs_path=None):
    """
    Checks if the HDFS path exists.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :return: False if the path exists, otherwise it returns True.
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")
    try:
        if hdfs_client.status(hdfs_path):
            return hdfs_path
    except:
        return False

def _checksum_from_local_file(file_name):
    hash_md5 = hashlib.md5()
    chunk_size = 64 * 1024
    with open(file_name, "rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def _checksum_from_hdfs_file(hdfs_client, hdfs_path):

    status_hdfs = hdfs_client.status(hdfs_path, strict=False)
    if status_hdfs is not None and status_hdfs['type'] == 'FILE':
        chunk_size = 64 * 1024
        hash_md5 = hashlib.md5()
        with hdfs_client.read(hdfs_path) as file:
            for chunk in iter(lambda: file.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    raise HdfsError("{0} provided is not file.".format(hdfs_path))

def change_owner(module, hdfs_client, hdfs_path=None, owner=None):
    """
    Sets the owner of an HDFS file or directory.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :param owner: owner
    :return: False if the owner of the HDFS file or directory is not changed, otherwise it returns True.
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")
    if owner is None:
        module.fail_json(path=owner, msg="owner should not be empty.")

    if _path_exists(module, hdfs_client, hdfs_path):
        current_owner = hdfs_client.status(hdfs_path)["owner"]
        hdfs_client.set_owner(hdfs_path, owner=owner)
        new_owner = hdfs_client.status(hdfs_path)["owner"]
        flag = (current_owner != new_owner)
        return {'current': current_owner, 'new': new_owner, 'changed': flag}
    else:
        module.fail_json(path=hdfs_path, msg="{0} - path doesn't exist".format(hdfs_path))

def change_group(module, hdfs_client, hdfs_path=None, group=None):
    """
    Sets the group of an HDFS file or directory.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :param group: group
    :return: False if the group of the HDFS file or directory is not changed, otherwise it returns True.
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")
    if group is None:
        module.fail_json(path=group, msg="group should not be empty.")

    if _path_exists(module, hdfs_client, hdfs_path):
        current_group = hdfs_client.status(hdfs_path)["group"]
        hdfs_client.set_owner(hdfs_path, group=group)
        new_group = hdfs_client.status(hdfs_path)["group"]
        flag = (current_group != new_group)
        return {'current': current_group, 'new': new_group, 'changed': flag}
    else:
        module.fail_json(path=hdfs_path, msg="{0} - path doesn't exist".format(hdfs_path))

def change_permission(module, hdfs_client, hdfs_path=None, permission=None):
    """
    Sets the permissions of an HDFS file or directory.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :param permission: permission (in octal string)
    :return: False if the permission of the HDFS file or directory is not changed, otherwise it returns True.
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")
    if permission is None:
        module.fail_json(path=permission, msg="permission should not be empty.")

    if _path_exists(module, hdfs_client, hdfs_path):
        current_permission = hdfs_client.status(hdfs_path)["permission"]
        hdfs_client.set_permission(hdfs_path, permission=permission)
        new_permission = hdfs_client.acl_status(hdfs_path)["permission"]
        flag = (current_permission != new_permission)
        return {'current': current_permission, 'new': new_permission, 'changed': flag}
    else:
        module.fail_json(path=hdfs_path, msg="{0} - path doesn't exist".format(hdfs_path))

def upload_localfile(module, hdfs_client, hdfs_path=None, local_path=None):
    """
    Uploads a local file in the HDFS directory. It checks if the file exists or not in the hdfs_path.
    If it doesn't exist, then uploads the file. If the file exists, then it checks if the content has changed.
    If the content hasn't changed, then it doesn't uploads the file. If the content has changed, overwrites the existing file.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :param local_path: local file path
    :return: False if the uploaded operation is not successful, otherwise it returns the HDFS path..
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")
    if local_path is None:
        module.fail_json(path=local_path, msg="local path should not be empty.")

    local_file_status = os.path.isfile(local_path)
    hdfs_dir_status = hdfs_client.status(hdfs_path, strict=False)

    if hdfs_dir_status is not None and hdfs_dir_status['type'] == 'DIRECTORY' and local_file_status is True:
        file_name = local_path.split("/")[-1]
        files_list = hdfs_client.list(hdfs_path)
        if file_name not in files_list:
            try:
                hdfs_client.upload(hdfs_path=hdfs_path, local_path=local_path)
                return True
            except (HdfsError, Exception) as e:
                module.fail_json(path=hdfs_path, local_path=local_path, msg="{0}".format(e))
        else:
            hdfs_file_path = os.path.join(hdfs_path, file_name)
            checksum_hdfs_file = _checksum_from_hdfs_file(hdfs_client, hdfs_file_path)
            checksum_local_file = _checksum_from_local_file(local_path)
            if str(checksum_hdfs_file) == str(checksum_local_file):
                module.exit_json(changed=False,
                                 local_path=local_path,
                                 path=hdfs_file_path,
                                 msg="content of the local file and the file in HDFS is same. Skipping upload.")
            else:
                try:
                    hdfs_client.upload(hdfs_path=hdfs_path, local_path=local_path, overwrite=True)
                    module.exit_json(changed=True,
                                     local_path=local_path,
                                     path=hdfs_file_path,
                                     msg="content of the local file and the file in HDFS is different. Overwriting the file.")
                    return True
                except (HdfsError, Exception) as e:
                    module.fail_json(path=hdfs_path, msg="{0}".format(e))
    else:
        module.fail_json(path=hdfs_path, local_path=local_path,
                         msg="either HDFS path provided is not a directory or local file provided is not a file.")

def create_directory(module, hdfs_client, hdfs_path):
    """
    Creates a new directory in the HDFS, if it does not exist.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :return: False if the directory already exists, otherwise it creates the directory and returns True.
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")

    # remove "/" at end, if present
    hdfs_path = hdfs_path[:len(hdfs_path)-1] if hdfs_path.endswith("/") else hdfs_path

    # collect dir_name and parent directory path from the HDFS path
    dir_name = hdfs_path.split("/")[-1]
    parent_dir = "/".join(hdfs_path.split("/")[:-1])
    files_list = hdfs_client.list(parent_dir)
    # checks if directory already exists
    if dir_name in files_list:
        return False
    else:
        try:
            hdfs_client.makedirs(hdfs_path)
            return True
        except (HdfsError, Exception) as e:
            module.fail_json(path=hdfs_path, msg="{0}".format(e))

def remove(module, hdfs_client, hdfs_path=None, recursive=False):
    """
    Removes a file or a directory if the HDFS path exists.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    :param hdfs_path: HDFS path
    :param recursive: if recursive is set to True, it recursively delete's subdirectories.
    :return: False if the remove operation is not successful, otherwise it returns True.
    """
    if hdfs_path is None:
        module.fail_json(path=hdfs_path, msg="HDFS path should not be empty.")

    if _path_exists(module, hdfs_client, hdfs_path):
        # remove "/" at end, if present
        hdfs_path = hdfs_path[:len(hdfs_path) - 1] if hdfs_path.endswith("/") else hdfs_path
        try:
            return hdfs_client.delete(hdfs_path, recursive=recursive)
        except (HdfsError, Exception) as e:
            module.fail_json(path=hdfs_path, msg="{0}".format(e))
    else:
        module.fail_json(path=hdfs_path, msg="{0} - path doesn't exist".format(hdfs_path))

def run(module, hdfs_client):
    """
    Run's the HDFS Ansible module operations and performs operations with the given HDFS command.
    :param module: Ansible module
    :param hdfs_client: HDFS client
    """
    params = module.params
    command = params['command']
    recurse = params['recurse']
    hdfs_path = params["path"]
    owner = params["owner"]
    group = params["group"]
    permission = params["permission"]
    local_path = params["local_path"]

    if command == "rm":
        result = remove(module, hdfs_client, hdfs_path=hdfs_path, recursive=recurse)
        if result == False:
            module.exit_json(changed=False,
                             path=hdfs_path,
                             msg="{0} - no such file or directory".format(hdfs_path))
        elif result == True:
            module.exit_json(changed=True,
                             path=hdfs_path,
                             msg="deleted: {0}".format(hdfs_path))

    elif command == "chown":
        result = change_owner(module, hdfs_client, hdfs_path=hdfs_path, owner=owner)
        if result['changed'] == False:
            module.exit_json(changed=False,
                             path=hdfs_path,
                             msg="previous owner: '{0}', current owner: '{1}' ".format(result['current'], result['new']))
        elif result['changed'] == True:
            module.exit_json(changed=True,
                             path=hdfs_path,
                             msg="previous owner: '{0}', current owner: '{1}' ".format(result['current'], result['new']))

    elif command == "chgrp":
        result = change_group(module, hdfs_client, hdfs_path=hdfs_path, group=group)
        if result['changed'] == False:
            module.exit_json(changed=False,
                             path=hdfs_path,
                             msg="previous group: '{0}', current group: '{1}' ".format(result['current'], result['new']))
        elif result['changed'] == True:
            module.exit_json(changed=True,
                             path=hdfs_path,
                             msg="previous group: '{0}', current group: '{1}' ".format(result['current'], result['new']))

    elif command == "chmod":
        result = change_permission(module, hdfs_client, hdfs_path=hdfs_path, permission=permission)
        if result['changed'] == False:
            module.exit_json(changed=False,
                             path=hdfs_path,
                             msg="previous permission: '{0}', current permission: '{1}' ".format(result['current'], result['new']))
        elif result['changed'] == True:
            module.exit_json(changed=True,
                             path=hdfs_path,
                             msg="previous permission: '{0}', current permission: '{1}' ".format(result['current'], result['new']))

    elif command == "put":
        file_name = local_path.split("/")[-1]
        file_path = os.path.join(hdfs_path, file_name)
        uploaded = upload_localfile(module, hdfs_client, hdfs_path=hdfs_path, local_path=local_path)
        if uploaded:
            module.exit_json(changed=True,
                             path=hdfs_path,
                             msg="uploaded: {0} ".format(file_path))

    elif command == "mkdir":
        created_dir = create_directory(module, hdfs_client, hdfs_path)
        if created_dir == False:
            module.exit_json(changed=False,
                             path=hdfs_path,
                             msg="directory already exists.")
        elif created_dir == True:
            module.exit_json(changed=True,
                             path=hdfs_path,
                             msg="created directory")

def main():
    """
    Main entry point of the execution.
    """
    fields = {
        "webhdfs_url": {"required": True, "type": "str"},
        "path": {"required": True, "type": "str"},
        "command": {"required": True,
                    "choices": ["rm", "chown", "chgrp", "chmod", "put", "mkdir"],
                    "type": "str"},
        "recurse": {"default": False, "type": "bool"},
        "local_path": {"required": False, "type": "str"},
        "owner": {"required": False, "type": "str"},
        "group": {"required": False, "type": "str"},
        "permission": {"required": False, "type": "str"},
    }

    module = AnsibleModule(argument_spec=fields, supports_check_mode=True)

    try:
        params = module.params
        webhdfs_url = params["webhdfs_url"]

        hdfs_client = InsecureClient(webhdfs_url)
        # hdfs_client = KerberosClient(webhdfs_url)
        run(module, hdfs_client)

    except Exception as e:
        module.fail_json(msg="Unable to init WEB HDFS client for {0}: {1}".format(webhdfs_url, str(e)))

if __name__ == '__main__':
    main()