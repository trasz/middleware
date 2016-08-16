#
# Copyright 2016 iXsystems, Inc.
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
######################################################################

import unittest
from freenas.dispatcher.client import Client
from schema import load_schema_definitions, verify_schema, get_schema, get_methods


class BaseTestCase(unittest.TestCase):
    def __init__(self, methodName):
        super(BaseTestCase, self).__init__(methodName)
        self.context = None

    def setUp(self):
        super(BaseTestCase, self).setUp()

        assert self.context is not None
        self.ssh_client = self.context.ssh_client
        self.client = Client()
        self.client.connect('ws://{0}'.format(self.context.hostname))
        self.client.login_user(self.context.username, self.context.password)
        load_schema_definitions(self.client)

    def tearDown(self):
        self.client.disconnect()

    def ssh_exec(self, command, output=False):
        _, stdout, stderr = self.ssh_client.exec_command(command)
        exitcode = stdout.channel.recv_exit_status()
        if output:
            return exitcode, stdout.read(), stderr.read()

        return exitcode

    def get_params_schema(self, method):
        return get_methods(self.client, method).get('params-schema')

    def get_result_schema(self, method):
        return get_methods(self.client, method).get('results-schema')

    def assertConformsToSchema(self, obj, schema, strict=False):
        errors = verify_schema(schema, obj, strict)
        if errors:
            raise AssertionError('Object {0} does not match {1} schema. Errors: {2}'.format(obj, schema, errors))

    def assertConformsToNamedSchema(self, obj, schema_name, strict=False):
        schema = get_schema(schema_name)
        if not schema:
            raise AssertionError('Schema {0} is unknown'.format(schema_name))
        self.assertConformsToSchema(obj, schema, strict)
