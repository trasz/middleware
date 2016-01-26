#+
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


def register_general_purpose_schemas(dispatcher):
    dispatcher.register_schema_definition('ip-address', {
        'anyOf': [
            {
                'type': 'string',
                'format': 'ipv4'
            },
            {
                'type': 'string',
                'format': 'ipv6'
            }
        ]
    })

    dispatcher.register_schema_definition('ipv4-address', {
        'type': 'string',
        'format': 'ipv4'
    })

    dispatcher.register_schema_definition('ipv6-address', {
        'type': 'string',
        'format': 'ipv6'
    })

    dispatcher.register_schema_definition('iso-datetime', {
        'type': 'string',
        'format': 'date-time'
    })

    dispatcher.register_schema_definition('email', {
        'type': 'string',
        'format': 'email'
    })

    dispatcher.register_schema_definition('task', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'args': {'type': 'object'},
            'id': {'type': 'integer'},
            'parent': {'type': ['integer', 'null']},
            'debugger': {'type': ['object', 'null']},
            'user': {'type': ['string', 'null']},
            'session': {'type': ['integer', 'null']},
            'resources': {'type': ['array', 'null']},
            'created_at': {'type': ['object', 'null']},
            'started_at': {'type': ['object', 'null']},
            'updated_at': {'type': ['object', 'null']},
            'finished_at': {'type': ['object', 'null']},
            'state': {
                'type': 'string',
                'enum': ['CREATED', 'WAITING', 'EXECUTING', 'ROLLBACK', 'FINISHED', 'FAILED', 'ABORTED']
            },
            'result': {'type': ['object', 'null']},
            'output': {'type': 'string'},
            'warnings': {
                'type': 'array',
                'items': 'string'
            },
            'error': {'type': ['object', 'null']},
            'rusage': {'type': ['object', 'null']}
        }
    })
