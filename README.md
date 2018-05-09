# Ansible

Ansible is software that automates software provisioning, configuration management, and application deployment.
[Wikipedia](https://en.wikipedia.org/wiki/Ansible_(software))

For managing the configuration, Ansible uses playbooks. The playbook is the core component of any Ansible configuration.
An Ansible playbook contains one or multiple plays, each of which define the work to be done for a configuration on a managed server.

Ansible plays are written in YAML [`github_repo` is custom module and `copy` is ansible module.]
Tasks are ordered in **List of Dictionaries**.
``` yaml
- hosts: localhost
  vars:
      file_name: inventory

  tasks:
    - name: Test that my module works
      github_repo:
      register: result

    - name: Debugging result of my module
      debug: var=result

    - name: Copy inventory file into /tmp/
      copy: src={{file_name}}
            dest=/tmp/{{file_name}}
```
[Yaml Basic](https://www.youtube.com/watch?v=o9pT9cWzbnI)\
[Playbook Basic](https://www.youtube.com/watch?v=Z01b9QZG0D0)\
More on [ansible playbooks](http://docs.ansible.com/ansible/latest/user_guide/playbooks.html).

Run playbook using following command:
```bash
ansible-playbook -i src/inventory src/play.yml
```

Majority of the work in here is based on [Custom Ansible Module](https://blog.toast38coza.me/custom-ansible-module-hello-world/)

This repository is about creating Ansible Modules that can `create or delete a repository on github`.

### Ansible Module:
Ansible modules are the building blocks for building ansible playbooks. They are small pieces of python code that can be triggered from the `yaml` in a playbook.

### Resources:

1. Understanding Ansible:
   * https://www.youtube.com/watch?v=Z01b9QZG0D0
   * https://www.youtube.com/watch?v=MfoAb50Br94
   * https://youtu.be/xMHVvHZ-Zn4?t=2m54s
   
1. Custom Ansible Module (using python):
   * https://blog.toast38coza.me/custom-ansible-module-hello-world/
   * https://www.relaxdiego.com/2016/09/writing-ansible-modules-002.html
   * https://rogerwelin.github.io/ansible/2016/04/25/creating-custom-ansible-modules.html
   * https://github.com/grantneale/ansible-modules-hdfs/blob/master/modules/hdfs/hdfsfile.py