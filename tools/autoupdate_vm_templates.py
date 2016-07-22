#!/usr/bin/env python
#
# Copyright iXsystems, Inc. 2016

import os
import sys
import json
import datetime

if len(sys.argv) < 2:
    exit('Please provide VM templates repository path')

for root, dirs, files in os.walk(sys.argv[1]):
    if 'template.json' in files:
        path = os.path.join(root, 'template.json')
        with open(path, 'r') as f:
            new = ''
            for line in f:
                if 'updated_at' in line:
                    new += '        "updated_at": {"$date": '
                    new += '"{0}"'.format(str(datetime.datetime.utcnow()).split('.')[0].replace(' ', 'T'))
                    new += '},\n'
                else:
                    new += line

        try:
            json.loads(new)
        except ValueError as e:
            exit('Error parsing {} template. Please check its syntax \n Error message: \n {}'.format(path, e))
        with open(path, 'w') as f:
            f.write(new)
