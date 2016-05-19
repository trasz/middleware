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
from freenas.dispatcher.rpc import RpcException, description
from lib.system import system, SubprocessException
from task import ProgressTask, TaskWarning, TaskDescription


@description('Collects debug information')
class CollectDebugTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        pass

    def describe(self, *args, **kwargs):
        pass

    def verify(self, fd):
        return ['system']

    def process_hook(self, cmd, plugin, tar):
        if cmd['type'] == 'AttachData':
            info = tarfile.TarInfo(os.path.join(plugin, cmd['name']))
            info.size = len(cmd['data'])
            tar.addfile(info, io.BytesIO(cmd['data'].encode('utf-8')))

        if cmd['type'] == 'AttachCommandOutput':
            try:
                out, err = system(*cmd['command'], shell=cmd['shell'])
                content = out + '\n' + err + '\n'
            except SubprocessException as err:
                content = 'Exit code: {0}\n'.format(err.returncode)
                content += 'stdout:\n'
                content += err.out
                content += 'stderr:\n'
                content += err.err

            info = tarfile.TarInfo(os.path.join(plugin, cmd['name']))
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content.encode('utf-8')))

        if cmd['type'] in ('AttachDirectory', 'AttachFile'):
            tar.add(cmd['path'], arcname=os.path.join(plugin, cmd['name']), recursive=cmd.get('recursive'))

    def run(self, fd):
        with os.fdopen(fd.fd, 'wb') as f:
            with tarfile.open(fileobj=f, mode='w:gz', dereference=True) as tar:
                plugins = self.dispatcher.call_sync('management.get_plugin_names')
                total = len(plugins)
                done = 0

                # Iterate over plugins
                for plugin in plugins:
                    self.set_progress(done / total * 100, 'Collecting debug info for {0}'.format(plugin))
                    try:
                        hooks = self.dispatcher.call_sync('management.collect_debug', plugin)
                    except RpcException as err:
                        self.add_warning(
                            TaskWarning(err.code, 'Cannot collect debug data for {0}: {1}'.format(plugin, err.message))
                        )
                        continue

                    for hook in hooks:
                        self.process_hook(hook, plugin, tar)

                    done += 1


@description('Saves debug information')
class SaveDebugTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        pass

    def describe(self, *args, **kwargs):
        pass

    def verify(self):
        return ['system']

    def run(self):
        pass


def _init(dispatcher, plugin):
    plugin.register_task_handler('debug.collect', CollectDebugTask)
    plugin.register_task_handler('debug.save', SaveDebugTask)
