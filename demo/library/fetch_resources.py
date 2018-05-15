#!/usr/bin/env python
# A module that fetches a resource pointed to by a URL and then writes it to disk.

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.urls import open_url
from urllib2 import URLError

class FetchError(Exception):
    pass

class WriteError(Exception):
    pass

def save_data(mod):
    data = fetch(mod.params["url"])
    write(data, mod.params["dest"])
    mod.exit_json(msg="Data saved", changed=True)

def fetch(url):
    try:
        stream = open_url(url)
        return stream.read()
    except URLError:
        raise FetchError("Data could not be fetched.")

def write(data, dest):
    try:
        with open(dest, "w") as dest:
            dest.write(data)
    except IOError:
        raise WriteError("Data could not be written.")

def main():
    mod = AnsibleModule(
        argument_spec=dict(
            url = dict(required=True),
            dest = dict(required=False, default='/tmp/testAnsible.txt')
        )
    )

    save_data(mod)

if __name__ == "__main__":
    main()