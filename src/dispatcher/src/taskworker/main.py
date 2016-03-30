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


import os
import sys
import errno
import setproctitle
import socket
import traceback
import logging
import queue
from threading import Event
from freenas.dispatcher.client import Client
from freenas.dispatcher.rpc import RpcService, RpcException, RpcWarning
from freenas.utils import load_module_from_file
from datastore import get_datastore
from datastore.config import ConfigStore


def serialize_error(err):
    etype, _, _ = sys.exc_info()
    stacktrace = traceback.format_exc() if etype else traceback.format_stack()

    ret = {
        'type': type(err).__name__,
        'message': str(err),
        'stacktrace': stacktrace
    }

    if isinstance(err, (RpcException, RpcWarning)):
        ret['code'] = err.code
        ret['message'] = err.message
        if err.extra:
            ret['extra'] = err.extra
    else:
        ret['code'] = errno.EFAULT

    return ret


class DispatcherWrapper(object):
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def __run_hook(self, name, args):
        return self.dispatcher.call_sync('task.run_hook', name, args, timeout=300)

    def __verify_subtask(self, task, name, args):
        return self.dispatcher.call_sync('task.verify_subtask', name, list(args))

    def __run_subtask(self, task, name, args):
        return self.dispatcher.call_sync('task.run_subtask', name, list(args), timeout=60)

    def __join_subtasks(self, *tasks):
        return self.dispatcher.call_sync('task.join_subtasks', tasks, timeout=None)

    def __abort_subtask(self, id):
        return self.dispatcher.call_sync('task.abort_subtask', id, timeout=60)

    def __add_warning(self, warning):
        self.dispatcher.call_sync('task.put_warning', serialize_error(warning))

    def __register_resource(self, resource, parents):
        self.dispatcher.call_sync('task.register_resource', resource.name, parents)

    def __unregister_resource(self, resource):
        self.dispatcher.call_sync('task.unregister_resource', resource)

    def __getattr__(self, item):
        if item == 'dispatch_event':
            return self.dispatcher.emit_event

        if item == 'run_hook':
            return self.__run_hook

        if item == 'verify_subtask':
            return self.__verify_subtask

        if item == 'run_subtask':
            return self.__run_subtask

        if item == 'join_subtasks':
            return self.__join_subtasks

        if item == 'abort_subtask':
            return self.__abort_subtask

        if item == 'add_warning':
            return self.__add_warning

        if item == 'register_resource':
            return self.__register_resource

        if item == 'unregister_resource':
            return self.__unregister_resource

        return getattr(self.dispatcher, item)


class TaskProxyService(RpcService):
    def __init__(self, context):
        self.context = context

    def get_status(self):
        self.context.running.wait()
        return self.context.instance.get_status()

    def set_master_progress_detail(self, detail):
        self.context.running.wait()
        self.context.instance.set_master_progress_detail(detail)

    def get_master_progress_info(self):
        self.context.running.wait()
        return self.context.instance.get_master_progress_info()

    def abort(self):
        if not hasattr(self.context.instance, 'abort'):
            raise RpcException(errno.ENOTSUP, 'Abort not supported')

        try:
            self.context.instance.abort()
        except BaseException as err:
            raise RpcException(errno.EFAULT, 'Cannot abort: {0}'.format(str(err)))

    def run(self, task):
        self.context.task.put(task)


class Context(object):
    def __init__(self):
        self.service = TaskProxyService(self)
        self.task = queue.Queue(1)
        self.datastore = None
        self.configstore = None
        self.conn = None
        self.instance = None
        self.running = Event()

    def put_status(self, state, result=None, exception=None):
        obj = {
            'status': state,
            'result': None
        }

        if result is not None:
            obj['result'] = result

        if exception is not None:
            obj['error'] = serialize_error(exception)

        self.conn.call_sync('task.put_status', obj)

    def main(self):
        if len(sys.argv) != 2:
            print("Invalid number of arguments", file=sys.stderr)
            sys.exit(errno.EINVAL)

        key = sys.argv[1]
        logging.basicConfig(level=logging.DEBUG)

        self.datastore = get_datastore()
        self.configstore = ConfigStore(self.datastore)
        self.conn = Client()
        self.conn.connect('unix:')
        self.conn.login_service('task.{0}'.format(os.getpid()))
        self.conn.enable_server()
        self.conn.rpc.register_service_instance('taskproxy', self.service)
        self.conn.call_sync('task.checkin', key)
        setproctitle.setproctitle('task executor (idle)')

        while True:
            try:
                task = self.task.get()
                setproctitle.setproctitle('task executor (tid {0})'.format(task['id']))

                if task['debugger']:
                    sys.path.append('/usr/local/lib/dispatcher/pydev')

                    import pydevd
                    host, port = task['debugger']
                    pydevd.settrace(host, port=port, stdoutToServer=True, stderrToServer=True)

                name, _ = os.path.splitext(os.path.basename(task['filename']))
                module = load_module_from_file(name, task['filename'])
                setproctitle.setproctitle('task executor (tid {0})'.format(task['id']))

                try:
                    self.instance = getattr(module, task['class'])(DispatcherWrapper(self.conn), self.datastore)
                    self.instance.configstore = self.configstore
                    self.instance.environment = task['environment']
                    self.running.set()
                    result = self.instance.run(*task['args'])
                except BaseException as err:
                    print("Task exception: {0}".format(str(err)), file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)

                    if hasattr(self.instance, 'rollback'):
                        self.put_status('ROLLBACK')
                        try:
                            self.instance.rollback(*task['args'])
                        except BaseException as rerr:
                            print("Task exception during rollback: {0}".format(str(rerr)), file=sys.stderr)
                            traceback.print_exc(file=sys.stderr)

                    self.put_status('FAILED', exception=err)
                else:
                    self.put_status('FINISHED', result=result)
                finally:
                    self.running.clear()

            except RpcException as err:
                print("RPC failed: {0}".format(str(err)), file=sys.stderr)
                sys.exit(errno.EBADMSG)
            except socket.error as err:
                print("Cannot connect to dispatcher: {0}".format(str(err)), file=sys.stderr)
                sys.exit(errno.ETIMEDOUT)

            if task['debugger']:
                import pydevd
                pydevd.stoptrace()

            setproctitle.setproctitle('task executor (idle)')


if __name__ == '__main__':
    ctx = Context()
    ctx.main()
