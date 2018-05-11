#!/usr/bin/env python

from ansible.module_utils.basic import AnsibleModule

# A module that fetches a resource pointed to by a URL and then writes it to disk.

def save_data(mod):
    raise NotImplementedError

def main():
    mod = AnsibleModule(
        argument_spec=dict(
            url = dict(required=True),
            dest = dict(required=False, default='/tmp/save_data')
        )
    )

    save_data(mod)

if __name__ == "__main__":
    main()