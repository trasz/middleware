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

from base import BaseTestCase


class TestUserQuery(BaseTestCase):
    def test_basic(self):
        result = self.client.call_sync('user.query')
        self.assertIsInstance(result, list)

    def test_filter_single(self):
        result = self.client.call_sync(
            'user.query',
            [('username', '=', 'root')],
            {'single': True}
        )

        self.assertIsInstance(result, dict)
        self.assertIn('username', result)
        self.assertEqual(result['username'], 'root')
        self.assertIn('uid', result)
        self.assertEqual(result['uid'], 0)


class TestUserCreate(BaseTestCase):
    def test_basic(self):
        task = self.client.call_task_sync('user.create', {
            'username': 'testuser1',
            'password': 'testpassword1',
            'group': 'wheel'
        })

        self.assertEqual(task['name'], 'user.create')
        self.assertEqual(task['state'], 'FINISHED')
        self.assertEqual(self.ssh_exec('id testuser1'), 0)

    def tearDown(self):
        test_users = self.client.call_sync(
            'user.query',
            [('username', '~', 'testuser.*')],
        )

        for u in test_users:
            self.client.call_task_sync('user.delete', u['id'])

        super(TestUserCreate, self).tearDown()
