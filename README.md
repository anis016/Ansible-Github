# Ansible

Ansible is software that automates software provisioning, configuration management, and application deployment.
[Wikipedia](https://en.wikipedia.org/wiki/Ansible_(software))

For managing the configuration, Ansible uses playbooks. The playbook is the core component of any Ansible configuration.
An Ansible playbook contains one or multiple plays, each of which define the work to be done for a configuration on a managed server.

Ansible plays are written in YAML [`hdfs_ansible` is custom module and `copy` is ansible module.]
Tasks are ordered in **List of Dictionaries**.
``` yaml 
- hosts: ubuntu_hosts
  vars:
      file_name: inventory

  tasks:
    - name: list all the files in the hdfs path
      hdfs_ansible:
        webhdfs_http_url: http://sandbox.hortonworks.com:50070
        hdfsPath: /tmp/
        recurse: False
        command: ls
      register: result_list

    - name: dump output for list
      debug:
        msg: '{{ result_list }}'

    - name: Copy inventory file into /tmp/
      copy: src={{file_name}}
            dest=/tmp/{{file_name}}
```
[Yaml Basic](https://www.youtube.com/watch?v=o9pT9cWzbnI)\
[Playbook Basic](https://www.youtube.com/watch?v=Z01b9QZG0D0)\
More on [ansible playbooks](http://docs.ansible.com/ansible/latest/user_guide/playbooks.html).

### HDFS Ansible Module:
Ansible modules are the building blocks for building ansible playbooks. They are small pieces of python code that can be triggered from the `yaml` in a playbook.

`hdfs_ansible` is the ansible module for interacting with HDFS. This modules uses the HTTP REST API for interfacing with HDFS.

### Resources:

Before running in the kerberos environment, kinit with `hdfs` user.
```bash
kinit -kt /etc/security/keytabs/hdfs.headless.keytab hdfs-sandbox@HORTONWORKS.COM
```

Once inside the directory, create and start the virual-environment as follows.
```bash
virtualenv ansible_venv
source ansible_venv/bin/activate
```

Install the `ansible` package
```bash
(ansible venv) pip install --trusted-host files.pythonhosted.org --trusted-host pypi.org --trusted-host pypi.python.org ansible
```

For running the playbook use the following command

1. If already in the `root` user.
```bash
ansible-playbook -i src/inventory src/play.yml
```
2. In sudo mode and passing the password inline. [not recommended]
```bash
ansible-playbook -i src/inventory src/play_apt.yml --user=sayed --extra-vars "ansible_sudo_pass=XXXXXXX"
```
3. In sudo mode without passing the password inline.
```bash
ansible-playbook -i src/inventory src/play_apt.yml --ask-become-pass
```
<!-- 
Majority of the work in here is based on [Custom Ansible Module](https://blog.toast38coza.me/custom-ansible-module-hello-world/) 
This repository is about creating Ansible Modules that can `create or delete a repository on github`.
-->

Run playbook using following command:


### Resources:

1. Understanding Ansible:
   * https://www.youtube.com/watch?v=Z01b9QZG0D0
   * https://www.youtube.com/watch?v=MfoAb50Br94
   * https://youtu.be/xMHVvHZ-Zn4?t=2m54s
   
2. Custom Ansible Module (using python):
   * https://blog.toast38coza.me/custom-ansible-module-hello-world/
   * https://www.relaxdiego.com/2016/09/writing-ansible-modules-002.html
   * https://rogerwelin.github.io/ansible/2016/04/25/creating-custom-ansible-modules.html
   * https://github.com/grantneale/ansible-modules-hdfs/blob/master/modules/hdfs/hdfsfile.py
   * https://linuxsimba.com/unit_testing_ansible_modules_part_1
   
3. Others:
   * http://docs.ansible.com/ansible/latest/user_guide/playbooks.html
   * http://docs.ansible.com/ansible/latest/dev_guide/developing_modules_general.html#new-module-development
   * http://docs.ansible.com/ansible/2.5/dev_guide/testing_units_modules.html
   * http://docs.ansible.com/ansible/latest/user_guide/intro_inventory.html 