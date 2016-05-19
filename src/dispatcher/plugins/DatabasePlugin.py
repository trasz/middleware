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
import json
from datastore import DatastoreException
from datastore.restore import restore_db
from freenas.dispatcher.rpc import description
from task import Task, ProgressTask, TaskException, TaskDescription


FACTORY_DB = '/usr/local/share/datastore/factory.json'


@description('Dumps current database state')
class DownloadDatabaseTask(Task):
    def verify(self):
        return ['system']

    def run(self):
        pass


@description('Uploads database state from file')
class UploadDatabaseTask(Task):
    def verify(self):
        return ['system']

    def run(self):
        pass


@description('Restores database config to it\'s defaults')
class RestoreFactoryConfigTask(ProgressTask):
    def verify(self):
        return ['root']

    def run(self):
        try:
            with open(FACTORY_DB, 'r') as fd:
                dump = json.load(fd)
        except IOError as err:
            raise TaskException(errno.ENOENT, "Cannot open input file: {0}".format(str(err)))
        except ValueError as err:
            raise TaskException(errno.EINVAL, "Cannot parse input file: {0}".format(str(err)))

        def progress(name):
            self.set_progress(50, 'Restored collection {0}'.format(name))

        try:
            restore_db(self.datastore, dump, progress_callback=progress)
        except DatastoreException as err:
            raise TaskException(errno.EFAULT, 'Cannot restore factory database: {0}'.format(str(err)))

        self.join_subtasks(self.run_subtask('system.reboot', 1))


def _init(dispatcher, plugin):
    plugin.register_task_handler('database.factory_restore', RestoreFactoryConfigTask)
