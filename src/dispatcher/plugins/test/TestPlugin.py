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
import time
from task import Task, TaskDescription, TaskWarning, ProgressTask, MasterProgressTask
from freenas.dispatcher.fd import FileDescriptor
from freenas.dispatcher.rpc import accepts, description


@description('Downloads tests')
class TestDownloadTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Downloading tests'

    def describe(self):
        return TaskDescription('Downloading tests')

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
    @classmethod
    def early_describe(cls):
        return 'Task Warning Tests'

    def describe(self):
        return TaskDescription('Testing Task Warnings')

    def verify(self):
        return []

    def run(self):
        self.add_warning(TaskWarning(errno.EBUSY, 'Warning 1'))
        self.add_warning(TaskWarning(errno.ENXIO, 'Warning 2'))
        self.add_warning(
            TaskWarning(errno.EINVAL, 'Warning 3 with extra payload', extra={'hello': 'world'})
        )


@accepts(int)
@description("Dummy Progress Task to test shit 1")
class ProgressChildTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Dummy Progress Task'

    def describe(self, duration):
        return TaskDescription('Dummy time.sleep based progress task ({0} secs)'.format(duration))

    def verify(self, duration):
        return ['system']

    def run(self, duration):
        self.message = "Executing {0} Task with duration of {1}...".format(
            self.__class__.__name__,
            duration
        )
        multiplier = 100.0 / duration
        for i in range(duration):
            time.sleep(1)
            self.set_progress((i + 1) * multiplier)


@accepts()
@description("Dummy Progess Master Task to test shit")
class ProgressMasterTask(MasterProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Dummy Master Progress Task'

    def describe(self):
        return TaskDescription(
            'Dummy MasterProgress Task that executes test.pchildtest twice with 5 second durations'
        )

    def verify(self):
        return ['system']

    def run(self):
        self.set_progress(0, 'Starting Master Progress Test Task...')
        self.join_subtasks(self.run_subtask('test.pchildtest', 1, weight=0.5))
        self.join_subtasks(self.run_subtask('test.pchildtest', 1, weight=0.5))


@accepts()
@description("Dummy Progess Master Task to test shit")
class NestedProgressMasterTask(MasterProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Dummy Master Progress Nested Task'

    def describe(self):
        return TaskDescription(
            'Dummy NestedMasterProgress Task that executes test.masterprogresstask & test.pchildtest'
        )

    def verify(self):
        return ['system']

    def run(self):
        self.set_progress(0, 'Starting Master Progress Test Task...')
        self.join_subtasks(self.run_subtask('test.masterprogresstask', weight=0.67))
        self.join_subtasks(self.run_subtask('test.pchildtest', 1, weight=0.33))


class ProgressTestTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Progress task test'

    def describe(self):
        return TaskDescription('Progress task test')

    def verify(self):
        return []

    def run(self):
        for i in range(0, 100):
            time.sleep(0.1)
            self.set_progress(i, 'fiddling')


class FailingTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Failing task test'

    def describe(self):
        return TaskDescription('Failing task test')

    def verify(self):
        return []

    def run(self):
        time.sleep(1)
        os.abort()


def _init(dispatcher, plugin):
    plugin.register_task_handler('test.test_download', TestDownloadTask)
    plugin.register_task_handler('test.test_warnings', TestWarningsTask)
    plugin.register_task_handler('test.pchildtest', ProgressChildTask)
    plugin.register_task_handler('test.masterprogresstask', ProgressMasterTask)
    plugin.register_task_handler('test.nestedmasterprogresstask', NestedProgressMasterTask)
    plugin.register_task_handler('test.progress', ProgressTestTask)
    plugin.register_task_handler('test.failing', FailingTask)
