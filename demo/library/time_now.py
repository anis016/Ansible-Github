#!/usr/bin/python

import datetime
import json

def show_time():
    date = str(datetime.datetime.now())
    json_dumps = json.dumps({
            "time": date
        })

    print json_dumps

show_time()