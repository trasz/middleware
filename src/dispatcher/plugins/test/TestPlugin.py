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
import threading
import uuid
import errno
from task import Task, TaskWarning
from freenas.dispatcher.fd import FileDescriptor


class TestDownloadTask(Task):
    def verify(self):
        return []

    def run(self):
        rfd, wfd = os.pipe()

        def feed():
            with os.fdopen(wfd, 'w') as f:
                for i in range(0, 100):
                    f.write(str(uuid.uuid4()) + '\n')

        t = threading.Thread(target=feed)
        t.start()
        url, = self.join_subtasks(self.run_subtask(
            'file.prepare_url_download', FileDescriptor(rfd)
        ))
        t.join(timeout=1)

        return url


class TestWarningsTask(Task):
    def verify(self):
        return []

    def run(self):
        self.add_warning(errno.EBUSY, 'Warning 1')
        self.add_warning(errno.ENXIO, 'Warning 2')
        self.add_warning(errno.EINVAL, 'Warning 3 with extra payload', extra={'hello': 'world'})


def _init(dispatcher, plugin):
    plugin.register_task_handler('test.test_download', TestDownloadTask)
    plugin.register_task_handler('test.test_warnings', TestWarningsTask)
