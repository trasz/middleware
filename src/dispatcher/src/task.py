#+
# Copyright 2014 iXsystems, Inc.
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

import copy
import errno
import logging
from freenas.dispatcher.rpc import RpcService, RpcException, RpcWarning
from freenas.utils import SmartEventSet
from datastore.config import ConfigStore
from threading import Event, Lock
import collections


class TaskState(object):
    CREATED = 'CREATED'
    WAITING = 'WAITING'
    EXECUTING = 'EXECUTING'
    ROLLBACK = 'ROLLBACK'
    FINISHED = 'FINISHED'
    FAILED = 'FAILED'
    ABORTED = 'ABORTED'


class Task(object):
    SUCCESS = (0, "Success")

    def __init__(self, dispatcher, datastore):
        self.dispatcher = dispatcher
        self.datastore = datastore
        self.configstore = ConfigStore(datastore)
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def _get_metadata(cls):
        return {
            'description': getattr(cls, 'description', None),
            'schema': getattr(cls, 'params_schema', None),
            'abortable': True if (hasattr(cls, 'abort') and isinstance(cls.abort, collections.Callable)) else False,
            'private': getattr(cls, 'private', False),
            'metadata': getattr(cls, 'metadata', None)
        }

    @classmethod
    def early_describe(cls):
        return cls.__name__

    def describe(self, *args, **kwargs):
        return

    def get_status(self):
        return TaskStatus(50, 'Executing...')

    def add_warning(self, warning):
        return self.dispatcher.add_warning(warning)

    def verify_subtask(self, classname, *args):
        return self.dispatcher.verify_subtask(self, classname, args)

    def run_subtask(self, classname, *args):
        return self.dispatcher.run_subtask(self, classname, args)

    def join_subtasks(self, *tasks):
        return self.dispatcher.join_subtasks(*tasks)

    def abort_subtask(self, id):
        return self.dispatcher.abort_subtask(id)

    def chain(self, task, *args):
        self.dispatcher.balancer.submit(task, *args)


class ProgressTask(Task):
    def __init__(self, dispatcher, datastore):
        super(ProgressTask, self).__init__(dispatcher, datastore)
        self.progress = 0
        self.message = 'EXECUTING...'

    def get_status(self):
        return TaskStatus(self.progress, self.message)

    def set_progress(self, percentage, message=None):
        self.progress = percentage
        if message:
            self.message = message


class ProgressSubtask(object):

    def __init__(self, *args, **kwargs):
        self.tid = kwargs.get('tid')
        if 'weight' in kwargs and 0.0 <= kwargs['weight'] <= 1.0:
            self.weight = kwargs['weight']
        else:
            self.weight = 1
        self.classname = kwargs.get('classname')
        self.joining = Event()
        self.ended = Event()
        self.result = None


class MasterProgressTask(ProgressTask):

    def __init__(self, dispatcher, datastore):
        super(MasterProgressTask, self).__init__(dispatcher, datastore)
        self.progress_subtasks = {}
        self.concurent_subtask_detail = {
            'tids': [],
            'average_weight': 1,
        }
        self.active_tids = []
        self.increment_progress = 0
        # only works well if a single subtask is executing at a time
        # else might lead to confusion
        self.pass_subtask_details = True
        self.state_lock = Lock()

    def get_master_progress_info(self):
        with self.state_lock:
            state_obj = {
                'subtask_weights': {x: v.weight for x, v in self.progress_subtasks.items()},
                'increment_progress': self.increment_progress,
                'active_tids': self.active_tids,
                'concurent_subtask_detail': self.concurent_subtask_detail,
                'progress': self.progress,
                'message': self.message,
                'pass_subtask_details': self.pass_subtask_details
            }
        return state_obj

    def set_master_progress_detail(self, detail):
        with self.state_lock:
            self.progress = int(detail.get('progress', self.progress))
            self.increment_progress = int(detail.get('increment_progress', self.increment_progress))

    def run_subtask(self, classname, *args, **kwargs):
        weight = kwargs.pop('weight', 1)
        tid = self.dispatcher.run_subtask(self, classname, args)
        subtask = ProgressSubtask(tid=tid, weight=weight, classname=classname)
        with self.state_lock:
            self.progress_subtasks[tid] = subtask
            self.active_tids.append(tid)
        return tid

    def join_subtasks(self, *tasks):
        subtask_results = []
        for tid in tasks:
            with SmartEventSet(self.progress_subtasks[tid].joining):
                result = self.dispatcher.join_subtasks(tid)
            with self.state_lock:
                self.progress_subtasks[tid].ended.set()
                self.progress_subtasks[tid].result = result
                subtask_results.extend(result)
                self.active_tids.remove(tid)
                self.increment_progress = self.progress_subtasks[tid].weight * 100
                if tid in self.concurent_subtask_detail['tids']:
                    self.concurent_subtask_detail['task_ids'].remove(tid)
                    self.increment_progress *= self.concurent_subtask_detail['average_weight']
                self.increment_progress = int(self.increment_progress)

        return subtask_results


class TaskException(RpcException):
    pass


class TaskAbortException(TaskException):
    pass


class TaskWarning(RpcWarning):
    pass


class ValidationException(TaskException):
    def __init__(self, errors=None, extra=None, **kwargs):
        super(ValidationException, self).__init__(errno.EBADMSG, 'Validation failed', extra=[])

        if errors:
            for path, code, message in errors:
                self.extra.append({
                    'path': list(path),
                    'code': code,
                    'message': message
                })

        if extra:
            self.extra = extra

    def add(self, path, message, code=errno.EINVAL):
        self.extra.append({
            'path': list(path),
            'code': code,
            'message': message
        })

    def propagate(self, other, src_path, dst_path):
        for err in other.extra:
            if err['path'][:len(src_path)] == src_path:
                new_err = copy.deepcopy(err)
                new_err['path'] = dst_path + err['path'][len(src_path):]
                self.extra.append(new_err)

    def __bool__(self):
        return bool(self.extra)


class VerifyException(TaskException):
    pass


class TaskStatus(object):
    def __init__(self, percentage, message=None, extra=None):
        self.percentage = percentage
        self.message = message
        self.extra = extra

    def __getstate__(self):
        return {
            'percentage': self.percentage,
            'message': self.message,
            'extra': self.extra
        }

    def __setstate__(self, obj):
        self.percentage = obj['percentage']
        self.message = obj['message']
        self.extra = obj['extra']


class TaskDescription(object):
    def __init__(self, fmt, **kwargs):
        self.kwargs = kwargs
        self.fmt = fmt

    def __str__(self):
        return self.fmt.format(**self.kwargs)

    def __getstate__(self):
        return {
            'message': str(self),
            'name': self.kwargs.get('name'),
            'format': {
                'string': self.fmt,
                'args': self.kwargs
            }
        }


class Provider(RpcService):
    def initialize(self, context):
        self.dispatcher = context.dispatcher
        self.datastore = self.dispatcher.datastore
        self.configstore = self.dispatcher.configstore


def metadata(**d):
    def wrapped(fn):
        fn.metadata = d

    return wrapped


def query(result_type):
    def wrapped(fn):
        fn.params_schema = [
            {
                'title': 'filter',
                'type': 'array',
                'items': {
                    'type': 'array',
                    'minItems': 2,
                    'maxItems': 4
                }
            },
            {
                'title': 'options',
                'type': 'object',
                'properties': {
                    'sort': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    },
                    'limit': {'type': 'integer'},
                    'offset': {'type': 'integer'},
                    'single': {'type': 'boolean'},
                    'count': {'type': 'boolean'}
                }
            }
        ]

        fn.result_schema = {
            'anyOf': [
                {
                    'type': 'array',
                    'items': {'$ref': result_type}
                },
                {
                    'type': 'integer'
                },
                {
                    '$ref': result_type
                }
            ]
        }

        return fn

    return wrapped
