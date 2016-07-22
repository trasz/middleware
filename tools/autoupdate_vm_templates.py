#!/usr/bin/env python
#
# Copyright iXsystems, Inc. 2016

import os
import sys
import json
import datetime
from collections import OrderedDict

if len(sys.argv) < 2:
    exit('Please provide VM templates repository path')

for root, dirs, files in os.walk(sys.argv[1]):
    if 'template.json' in files:
        path = os.path.join(root, 'template.json')
        try:
            with open(path, 'r') as f:
                template = json.load(f, object_pairs_hook=OrderedDict)
        except ValueError as e:
            exit('Error parsing {} template. Please check its syntax \n Error message: \n {}'.format(path, e))

        template['template']['updated_at']['$date'] = datetime.datetime.utcnow().isoformat().split('.')[0]

        with open(path, 'w') as f:
            json.dump(template, f, indent=4)
