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


class TestVolumeQuery(BaseTestCase):
    def test_basic(self):
        result = self.client.call_sync('volume.query')
        self.assertIsInstance(result, list)
        self.assertConformsToSchema(result, self.get_result_schema('volume.query'))


class TestVolumeCreateAuto(BaseTestCase):
    def test_basic(self):
        empty_disks = self.client.call_sync('disk.query', [('status.empty', '=', True)])
        self.assertGreater(len(empty_disks), 0, msg='No empty disks left on target machine')
        self.assertConformsToSchema(empty_disks, self.get_result_schema('disk.query'))
        task = self.client.call_task_sync(
            'volume.create_auto',
            'test_volume',
            'zfs',
            'disk',
            empty_disks
        )

        self.assertEqual(task['name'], 'volume.create_auto')
        self.assertEqual(task['state'], 'FINISHED')
        self.assertEqual(self.ssh_exec('zfs list test_volume'), 0)

    def tearDown(self):
        test_volumes = self.client.call_sync(
            'volume.query',
            [('id', '~', 'test_volume')],
        )

        for v in test_volumes:
            self.client.call_task_sync('volume.delete', v['id'])

        super(TestVolumeCreateAuto, self).tearDown()
