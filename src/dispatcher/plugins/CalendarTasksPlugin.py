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

import errno
from freenas.dispatcher.rpc import RpcException, description, accepts, returns, generator
from freenas.dispatcher.rpc import SchemaHelper as h
from task import Provider, Task, TaskException, query, TaskDescription
from lib.system import system, SubprocessException
from freenas.utils import query as q


@description('Provides information about calendar tasks')
class CalendarTasksProvider(Provider):
    @query('calendar-task')
    @generator
    def query(self, filter=None, params=None):
        return q.query(
            self.dispatcher.call_sync('scheduler.management.query'),
            *(filter or []),
            stream=True,
            **(params or {})
        )


@accepts(
    h.all_of(
        h.ref('calendar-task'),
        h.required('name'),
        h.no(h.required('status'))
    )
)
@returns(str)
@description('Creates a calendar task')
class CreateCalendarTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating calendar task"

    def describe(self, task):
        return TaskDescription("Creating calendar task {name}", name=task['name'])

    def verify(self, task):
        return ['system']

    def run(self, task):
        if task['name'] in self.dispatcher.call_sync('scheduler.management.query', [], {'select': 'name'}):
            raise TaskException(errno.EEXIST, 'Task {0} already exists'.format(task['name']))

        try:
            tid = self.dispatcher.call_sync('scheduler.management.add', task)
        except RpcException:
            raise

        self.dispatcher.dispatch_event('calendar_task.changed', {
            'operation': 'create',
            'ids': [tid]
        })


@accepts(
    str,
    h.all_of(
        h.ref('calendar-task'),
        h.no(h.required('status'))
    )
)
@description('Updates a calendar task')
class UpdateCalendarTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating calendar task"

    def describe(self, id, updated_params):
        return TaskDescription("Updating calendar task {name}", name=id)

    def verify(self, id, updated_params):
        return ['system']

    def run(self, id, updated_params):
        try:
            self.dispatcher.call_sync('scheduler.management.update', id, updated_params)
        except RpcException:
            raise

        self.dispatcher.dispatch_event('calendar_task.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str)
@description('Deletes a calendar task')
class DeleteCalendarTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting calendar task"

    def describe(self, id):
        return TaskDescription("Deleting calendar task {name}", name=id)

    def verify(self, id):
        return ['system']

    def run(self, id):
        try:
            self.dispatcher.call_sync('scheduler.management.delete', id)
        except RpcException:
            raise

        self.dispatcher.dispatch_event('calendar_task.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
@description("Runs the calendar task specified by the given id")
class RunCalendarTask(Task):
    @classmethod
    def early_describe(cls):
        return "Starting calendar task"

    def describe(self, id):
        return TaskDescription("Starting calendar task {name}", name=id)

    def verify(self, id):
        return ['system']

    def run(self, id):
        try:
            self.dispatcher.call_sync('scheduler.management.run', id)
        except RpcException:
            raise


@accepts(str, str)
@description('Runs a shell command as a specified user')
class CommandTask(Task):
    @classmethod
    def early_describe(cls):
        return "Starting shell command"

    def describe(self, user, command):
        return TaskDescription("Starting command {name} as {user}", name=command, user=user)

    def verify(self, user, command):
        return ['system']

    def run(self, user, command):
        try:
            out, err = system('/usr/bin/su', '-m', user, '-c', '/bin/sh', '-c', command)
        except SubprocessException as err:
            raise TaskException(errno.EFAULT, 'Command failed')

        print(out)


def _init(dispatcher, plugin):
    plugin.register_provider('calendar_task', CalendarTasksProvider)
    plugin.register_task_handler('calendar_task.create', CreateCalendarTask)
    plugin.register_task_handler('calendar_task.update', UpdateCalendarTask)
    plugin.register_task_handler('calendar_task.delete', DeleteCalendarTask)
    plugin.register_task_handler('calendar_task.run', RunCalendarTask)
    plugin.register_task_handler('calendar_task.command', CommandTask)
    plugin.register_event_type('calendar_task.changed')
