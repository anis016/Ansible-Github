#!/usr/bin/env python

from ansible.module_utils.basic import AnsibleModule

# A module that fetches a resource pointed to by a URL and then writes it to disk.

def save_data(mod):
    data = fetch(mod.params["url"])
    write(data, mod.params["dest"])
    mod.exit_json(msg="Data saved", changed=True)

def fetch(url):
    print "Fetch something"

def write(data, dest):
    print "Write the data"

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