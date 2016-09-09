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
#####################################################################

import os
import errno
import libzfs
import bsd
from datetime import datetime
from task import Provider, Task, TaskDescription, TaskException, ProgressTask
from freenas.utils.permissions import get_type, get_unix_permissions


class IndexProvider(Provider):
    def query(self, filter=None, params=None):
        pass


class IndexVolumeTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Indexing a volume"

    def describe(self, volume):
        return TaskDescription("Indexing volume {name}", name=volume)

    def verify(self, volume):
        return ['zpool:{0}'.format(volume)]

    def run(self, volume):
        self.run_subtask('volume.snapshot.create', {
            'dataset': volume,
            'name': 'org.freenas.indexer:now',
            'hidden': True
        }, True)

        # Gather a list of datasets
        for ds in self.dispatcher.call_sync('volume.dataset.query', [('volume', '=', volume)]):
            pass


class IndexDatasetIncrementalTask(ProgressTask):
    pass


class IndexDatasetFullTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Indexing a dataset"

    def describe(self, dataset):
        return TaskDescription("Indexing dataset {name}", name=dataset)

    def verify(self, dataset):
        return ['zfs:{0}'.format(dataset)]

    def run(self, dataset):
        mountpoint = self.dispatcher.call_sync('volume.get_dataset_path', dataset)

        # Estimate number of files
        statfs = bsd.statfs(mountpoint)
        total_files = statfs.files - statfs.free_files
        done_files = 0

        for root, dirs, files in os.walk(mountpoint):
            for d in dirs:
                path = os.path.join(root, d)
                collect(self.datastore, path)
                done_files += 1
                self.set_progress(done_files / total_files * 100, 'Processing directory {0}'.format(path))

            for f in files:
                path = os.path.join(root, f)
                collect(self.datastore, path)
                done_files += 1


def collect(datastore, path):
    try:
        st = os.stat(path, follow_symlinks=False)
    except OSError as err:
        # Can't access the file - delete index entry
        datastore.delete('fileindex', path)
        return

    volume = path.split('/')[2]
    datastore.upsert('fileindex', path, {
        'id': path,
        'volume': volume,
        'type': get_type(st),
        'atime': datetime.utcfromtimestamp(st.st_atime),
        'mtime': datetime.utcfromtimestamp(st.st_mtime),
        'ctime': datetime.utcfromtimestamp(st.st_ctime),
        'uid': st.st_uid,
        'gid': st.st_gid,
        'permissions': get_unix_permissions(st.st_mode)
    })


def _init(dispatcher, plugin):
    plugin.register_schema_definition('file-index', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'volume': {'type': 'string'},
            'type': {'type': 'string'},
            'ctime': {'type': 'datetime'},
            'mtime': {'type': 'datetime'},
            'atime': {'type': 'datetime'},
            'size': {'type': 'integer'},
            'uid': {'type': 'integer'},
            'gid': {'type': 'integer'},
            'permissions': {'$ref': 'permissions'}
        }
    })

    plugin.register_provider('index', IndexProvider)
    plugin.register_task_handler('index.generate', IndexVolumeTask)
    plugin.register_task_handler('index.generate.dataset.full', IndexDatasetFullTask)
