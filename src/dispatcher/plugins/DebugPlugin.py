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
import io
import tarfile
import errno
import logging
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from freenas.dispatcher.fd import FileDescriptor
from lib.system import system, SubprocessException
from task import (
    Provider, Task, ProgressTask, TaskWarning, TaskDescription, ValidationException, TaskException
)

logger = logging.getLogger('DebugPlugin')


class RemoteDebugProvider(Provider):
    @returns(h.ref('remote-debug-status'))
    def get_status(self):
        return self.dispatcher.call_sync('debugd.management.status')


@private
@description('Collects debug information')
class CollectDebugTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Collecting debug data'

    def describe(self, fd):
        return TaskDescription('Collecting debug data')

    def verify(self, fd):
        return ['system']

    def process_hook(self, cmd, plugin, tar):
        if cmd['type'] == 'AttachData':
            info = tarfile.TarInfo(os.path.join(plugin, cmd['name']))
            info.size = len(cmd['data'])
            tar.addfile(
                info,
                io.BytesIO(
                    cmd['data'] if isinstance(cmd['data'], bytes) else cmd['data'].encode('utf-8')
                )
            )

        if cmd['type'] == 'AttachCommandOutput':
            try:
                out, err = system(*cmd['command'], shell=cmd['shell'])
                content = out + '\n' + err + '\n'
            except SubprocessException as err:
                content = 'Exit code: {0}\nstdout:\n{1}stderr:\n{2}'.format(
                    err.returncode, err.out, err.err
                )

            info = tarfile.TarInfo(os.path.join(plugin, cmd['name']))
            info.size = len(content)
            tar.addfile(
                info,
                io.BytesIO(content if isinstance(content, bytes) else content.encode('utf-8'))
            )

        if cmd['type'] in ('AttachDirectory', 'AttachFile'):
            try:
                tar.add(
                    cmd['path'],
                    arcname=os.path.join(plugin, cmd['name']),
                    recursive=cmd.get('recursive')
                )
            except OSError as err:
                self.add_warning(TaskWarning(err.errno, 'Cannot file {0}: {1}'.format(cmd['path'], err.strerror)))

    def run(self, fd):
        try:
            with os.fdopen(fd.fd, 'wb') as f:
                with tarfile.open(fileobj=f, mode='w:gz', dereference=True) as tar:
                    plugins = self.dispatcher.call_sync('management.get_plugin_names')
                    total = len(plugins)
                    done = 0

                    # Iterate over plugins
                    for plugin in plugins:
                        self.set_progress(done / total * 100, 'Collecting debug info for {0}'.format(plugin))
                        try:
                            hooks = self.dispatcher.call_sync('management.collect_debug', plugin, timeout=600)
                        except RpcException as err:
                            self.add_warning(
                                TaskWarning(err.code, 'Cannot collect debug data for {0}: {1}'.format(plugin, err.message))
                            )
                            continue

                        for hook in hooks:
                            self.process_hook(hook, plugin, tar)

                        done += 1

        except BrokenPipeError as err:
            raise TaskException(errno.EPIPE, 'The download timed out') from err


@accepts(str)
@description('Saves debug information in a gzip format to file specified by user')
class SaveDebugTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Saving debug data to file in gzip format'

    def describe(self, path):
        return TaskDescription('Saving debug data to file: {filepath} in gzip format', filepath=path)

    def verify(self, path):
        errors = ValidationException()
        if path in [None, ''] or path.isspace():
            errors.add((0, 'path'), 'The Path is required', code=errno.EINVAL)
        if errors:
            raise errors
        return ['system']

    def run(self, path):
        file = open(path, 'wb+')
        self.join_subtasks(self.run_subtask(
            'debug.collect',
            FileDescriptor(file.fileno()),
            progress_callback=lambda p, m, e=None: self.chunk_progress(0, 100, '', p, m, e)
        ))


@description('Connects to the support server')
class RemoteDebugConnectTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Connecting to the support server'

    def describe(self, connect):
        return TaskDescription('Connecting to the support server')

    def verify(self, connect):
        return []

    def run(self, connect):
        self.dispatcher.call_sync('debugd.management.connect')


@description('Disconnects from the support server')
class RemoteDebugDisconnectTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Disconnecting from the support server'

    def describe(self, connect):
        return TaskDescription('Disconnecting from the support server')

    def verify(self, connect):
        return []

    def run(self, connect):
        self.dispatcher.call_sync('debugd.management.disconnect')


def _init(dispatcher, plugin):
    plugin.register_schema_definition('remote-debug-status', {
        'type': 'object',
        'additionalProperties': False,
        'readOnly': True,
        'properties': {
            'state': {
                'type': 'string',
                'enum': ['OFFLINE', 'CONNECTING', 'CONNECTED', 'LOST']
            },
            'server': {'type': 'string'},
            'connection_id': {'type': 'string'},
            'connected_at': {'type': 'datetime'},
            'jobs': {'type': 'array'}
        }
    })

    plugin.register_provider('debug.remote', RemoteDebugProvider)
    plugin.register_task_handler('debug.remote.connect', RemoteDebugConnectTask)
    plugin.register_task_handler('debug.remote.disconnect', RemoteDebugDisconnectTask)
    plugin.register_task_handler('debug.collect', CollectDebugTask)
    plugin.register_task_handler('debug.save_to_file', SaveDebugTask)
