#
# Copyright 2015 iXsystems, Inc.
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#####################################################################

import errno
import logging

from freenas.dispatcher.rpc import (
    accepts,
    description,
    returns,
    SchemaHelper as h
)

from resources import Resource 

from task import (
    query,
    Provider,
    Task,
    VerifyException
)

logger = logging.getLogger('DirectoryServicePlugin')


class DirectoryServicesProvider(Provider):
    @query('directoryservice')
    def query(self, filter=None, params=None):
        def extend(directory):
            return directory

        return self.datastore.query('directories', *(filter or []), callback=extend, **(params or {}))


class DirectoryServiceUpdateTask(Task):
    def verify(self, id, updated_params):
        return ['system']

    def run(self, id, updated_params):
        pass


def _init(dispatcher, plugin):
    plugin.register_schema_definition('directoryservice',  {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'priority': {'type': 'integer'},
            'plugin': {'type': 'string'},
            'enabled': {'type': 'boolean'},
            'uid_range': {
                'type': 'array',
                'items': [
                    {'type': 'integer'},
                    {'type': 'integer'}
                ]
            },
            'gid_range': {
                'type': 'array',
                'items': [
                    {'type': 'integer'},
                    {'type': 'integer'}
                ]
            },
            'parameters': {
                'type': 'object'
            },
            'status': {
                'type': 'object'
            }
        }
    })

    plugin.register_provider('directory', DirectoryServicesProvider)
    plugin.register_task_handler('directory.update', DirectoryServiceUpdateTask)
