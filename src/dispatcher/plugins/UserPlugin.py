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

import crypt
import copy
import errno
import os
import random
import string
import re
from task import Provider, Task, TaskException, TaskDescription, TaskWarning, ValidationException, VerifyException, query
from debug import AttachFile
from freenas.dispatcher.rpc import RpcException, description, accepts, returns, SchemaHelper as h, generator
from datastore import DuplicateKeyException, DatastoreException
from lib.system import SubprocessException, system
from freenas.utils import normalize


EMAIL_REGEX = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]*[a-zA-Z0-9]\.[a-zA-Z]{2,4}\b")


def check_unixname(name):
    """Helper method to check if a given name is a valid unix name
        1. Cannot start with dashes
        2. $ is only valid as a final character
        3. Cannot contain any of the following:  ,\t:+&#%\^()!@~\*?<>=|\\/"

    Returns: an array of errors [composed of a tuple (error code, error message)]
    """

    if not name.strip():
        yield errno.EINVAL, 'Name cannot be empty'

    if name.startswith('-'):
        yield errno.EINVAL, 'Your name cannot start with "-"'

    if name.find('$') not in (-1, len(name) - 1):
        yield errno.EINVAL, 'The character $ is only allowed as the final character'

    invalids = []
    for char in name:
        if char in ' ,\t:+&#%\^()!@~\*?<>=|\\/"' and char not in invalids:
            invalids.append(char)

    if invalids:
        yield errno.EINVAL, 'Your name contains invalid characters ({0}).'.format(''.join(invalids))


def crypted_password(cleartext):
    return crypt.crypt(cleartext, '$6$' + ''.join([
        random.choice(string.ascii_letters + string.digits) for _ in range(16)]))


@description("Provides access to users database")
class UserProvider(Provider):
    @description("Lists users present in the system")
    @query('user')
    @generator
    def query(self, filter=None, params=None):
        def extend(user):
            # If there's no 'attributes' property, put empty dict in that place
            if 'attributes' not in user:
                user['attributes'] = {}

            # If there's no 'groups' property, put empty array in that place
            if 'groups' not in user:
                user['groups'] = []

            return user

        return self.datastore.query_stream('users', *(filter or []), callback=extend, **(params or {}))

    def get_profile_picture(self, uid):
        pass

    @description("Retrieve the next UID available")
    @returns(int)
    def next_uid(self, check_gid=False):
        start_uid, end_uid = self.dispatcher.configstore.get('accounts.local_uid_range')
        uid = None
        for i in range(start_uid, end_uid):
            if check_gid and self.datastore.exists('groups', ('gid', '=', i)):
                continue

            if not self.datastore.exists('users', ('uid', '=', i)):
                uid = i
                break

        if not uid:
            raise RpcException(errno.ENOSPC, 'No free UIDs available')

        return uid


@description("Provides access to groups database")
class GroupProvider(Provider):
    @description("Lists groups present in the system")
    @query('group')
    @generator
    def query(self, filter=None, params=None):
        def extend(group):
            group['members'] = [x['id'] for x in self.datastore.query(
                'users',
                ('or', (
                    ('groups', 'in', group['id']),
                    ('group', '=', group['id'])
                ))
            )]
            return group

        return self.datastore.query_stream('groups', *(filter or []), callback=extend, **(params or {}))

    @description("Retrieve the next GID available")
    @returns(int)
    def next_gid(self):
        start_gid, end_gid = self.dispatcher.configstore.get('accounts.local_gid_range')
        gid = None
        for i in range(start_gid, end_gid):
            if not self.datastore.exists('groups', ('gid', '=', i)):
                gid = i
                break

        if not gid:
            raise RpcException(errno.ENOSPC, 'No free GIDs available')

        return gid


@description("Create an user in the system")
@accepts(h.all_of(
    h.ref('user'),
    h.required('username'),
    h.forbidden('builtin'),
    h.object(properties={'password': {'type': ['string', 'null']}}),
    h.any_of(
        h.required('password'),
        h.required('unixhash', 'smbhash'),
        h.required('password_disabled')
    )
))
class UserCreateTask(Task):
    def __init__(self, dispatcher, datastore):
        super(UserCreateTask, self).__init__(dispatcher, datastore)
        self.id = None
        self.created_group = False

    @classmethod
    def early_describe(cls):
        return "Creating user"

    def describe(self, user):
        return TaskDescription("Adding user {name}", name=user['username'])

    def verify(self, user):
        errors = ValidationException()

        for code, message in check_unixname(user['username']):
            errors.add((0, 'username'), message, code=code)

        if self.datastore.exists('users', ('username', '=', user['username'])):
            raise VerifyException(errno.EEXIST, 'User with given name already exists')

        if 'groups' in user and len(user['groups']) > 64:
            errors.add(
                (0, 'groups'),
                'User cannot belong to more than 64 auxiliary groups'
            )

        if user.get('full_name') and ':' in user['full_name']:
            errors.add((0, 'full_name'), 'The character ":" is not allowed')

        if 'email' in user:
            if not EMAIL_REGEX.match(user['email']):
                errors.add(
                    (0, 'email'),
                    "{0} is an invalid email address".format(user['email'])
                )

        if errors:
            raise errors

        return ['system']

    def run(self, user):
        if 'uid' not in user:
            # Need to get next free UID
            uid = self.dispatcher.call_sync('user.next_uid', user.get('group') is None)
        else:
            uid = user.pop('uid')

        normalize(user, {
            'builtin': False,
            'unixhash': '*',
            'full_name': 'User &',
            'shell': '/bin/sh',
            'home': '/nonexistent',
            'groups': [],
            'uid': uid,
            'attributes': {}
        })

        password = user.pop('password', None)
        if password:
            user['unixhash'] = crypted_password(password)

        if user.get('group') is None:
            try:
                result = self.join_subtasks(self.run_subtask('group.create', {
                    'gid': uid,
                    'name': user['username']
                }))
            except RpcException as err:
                raise err

            user['group'] = result[0]
            self.created_group = result[0]

        try:
            id = self.datastore.insert('users', user)
            self.id = id
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')

            if password:
                system(
                    '/usr/local/bin/smbpasswd', '-D', '0', '-s', '-a', user['username'],
                    stdin='{0}\n{1}\n'.format(password, password)
                )

                user['smbhash'] = system('/usr/local/bin/pdbedit', '-d', '0', '-w', user['username'])[0]
                self.datastore.update('users', id, user)

        except SubprocessException as e:
            raise TaskException(
                errno.ENXIO,
                'Could not generate samba password. stdout: {0}\nstderr: {1}'.format(e.out, e.err)
            )
        except DuplicateKeyException as e:
            raise TaskException(errno.EBADMSG, 'Cannot add user: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(
                errno.ENXIO,
                'Cannot regenerate users file, maybe etcd service is offline. Actual Error: {0}'.format(e)
            )

        volumes_root = self.dispatcher.call_sync('volume.get_volumes_root')
        if user['home'].startswith(volumes_root):
            if not os.path.exists(user['home']):
                try:
                    self.dispatcher.call_sync('volume.decode_path', user['home'])
                except RpcException as err:
                    raise TaskException(err.code, err.message)
                os.makedirs(user['home'])

            group = self.datastore.get_by_id('groups', user['group'])
            os.chown(user['home'], uid, group['gid'])
            os.chmod(user['home'], 0o755)
        elif not user['builtin'] and user['home'] not in (None, '/nonexistent'):
            raise TaskException(
                errno.ENOENT,
                "Invalid mountpoint specified for home directory: {0}.".format(user['home']) +
                " Use '{0}' instead as the mountpoint".format(volumes_root)
            )

        self.dispatcher.dispatch_event('user.changed', {
            'operation': 'create',
            'ids': [id]
        })

        return uid

    def rollback(self, user):
        if self.datastore.exists('users', ('id', '=', self.id)):
            self.datastore.delete('users', self.id)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')

        if self.created_group:
            self.join_subtasks(self.run_subtask('group.delete', self.created_group))


@description("Deletes an user from the system")
@accepts(str)
class UserDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting user"

    def describe(self, id):
        user = self.datastore.get_by_id('users', id)
        return TaskDescription("Deleting user {name}", name=user['username'] if user else id)

    def verify(self, id):
        user = self.datastore.get_by_id('users', id)

        if user is None:
            raise VerifyException(errno.ENOENT, 'User with UID {0} does not exist'.format(id))

        if user['builtin']:
            raise VerifyException(errno.EPERM, 'Cannot delete builtin user {0}'.format(user['username']))

        return ['system']

    def run(self, id):
        try:
            user = self.datastore.get_by_id('users', id)
            group = self.datastore.get_by_id('groups', user['group'])
            if group and user['uid'] == group['gid']:
                self.add_warning(TaskWarning(
                    errno.EBUSY,
                    'Group {0} ({1}) left behind, you need to delete it separately'.format(group['name'], group['gid']))
                )

            if user.get('smbhash'):
                try:
                    system('/usr/local/bin/pdbedit', '-x', user['username'])
                except SubprocessException as e:
                    pass

            self.datastore.delete('users', id)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot delete user: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('user.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@description('Updates an user')
@accepts(
    str,
    h.all_of(
        h.ref('user'),
        h.forbidden('builtin')
    )
)
class UserUpdateTask(Task):
    def __init__(self, dispatcher, datastore):
        super(UserUpdateTask, self).__init__(dispatcher, datastore)
        self.original_user = None

    @classmethod
    def early_describe(cls):
        return "Updating user"

    def describe(self, id, updated_fields):
        user = self.datastore.get_by_id('users', id)
        return TaskDescription("Updating user {name}", name=user['username'] if user else id)

    def verify(self, id, updated_fields):
        user = self.datastore.get_by_id('users', id)
        errors = ValidationException()

        if user is None:
            errors.add(
                (1, 'id'), "User with id: {0} does not exist".format(id), code=errno.ENOENT
            )
            raise errors

        if user.get('builtin'):
            if 'home' in updated_fields:
                errors.add(
                    (1, 'home'), "Cannot change builtin user's home directory", code=errno.EPERM
                )

            # Similarly ignore uid changes for builtin users
            if 'uid' in updated_fields:
                errors.add((1, 'uid'), "Cannot change builtin user's UID", code=errno.EPERM)

            if 'username' in updated_fields:
                errors.add(
                    (1, 'username'), "Cannot change builtin user's username", code=errno.EPERM
                )

            if 'locked' in updated_fields:
                errors.add(
                    (1, 'locked'), "Cannot change builtin user's locked flag", code=errno.EPERM
                )

        if updated_fields.get('full_name') and ':' in updated_fields['full_name']:
            errors.add((1, 'full_name'), 'The character ":" is not allowed')

        if 'groups' in updated_fields and len(updated_fields['groups']) > 64:
            errors.add((1, 'groups'), 'User cannot belong to more than 64 auxiliary groups')

        if 'username' in updated_fields:
            if self.datastore.exists('users', ('username', '=', updated_fields['username']), ('id', '!=', id)):
                errors.add((1, 'username'), 'Different user with given name already exists')

            for code, message in check_unixname(updated_fields['username']):
                errors.add((1, 'username'), message, code=code)

        if updated_fields.get('email'):
            if not EMAIL_REGEX.match(updated_fields['email']):
                errors.add(
                    (1, 'email'),
                    "{0} is an invalid email address".format(updated_fields['email'])
                )

        if errors:
            raise errors

        return ['system']

    def run(self, id, updated_fields):
        try:
            user = self.datastore.get_by_id('users', id)
            if not user:
                raise TaskException(errno.ENOENT, 'User {0} not found'.format(id))

            self.original_user = copy.deepcopy(user)

            home_before = user.get('home')
            user.update(updated_fields)

            password = user.pop('password', None)
            if password:
                user['unixhash'] = crypted_password(password)

            self.datastore.update('users', user['id'], user)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')

            if password:
                system(
                    '/usr/local/bin/smbpasswd', '-D', '0', '-s', '-a', user['username'],
                    stdin='{0}\n{1}\n'.format(password, password)
                )
                user['smbhash'] = system('/usr/local/bin/pdbedit', '-d', '0', '-w', user['username'])[0]
                self.datastore.update('users', id, user)

        except SubprocessException as e:
            raise TaskException(
                errno.ENXIO,
                'Could not generate samba password. stdout: {0}\nstderr: {1}'.format(e.out, e.err))
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot update user: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot regenerate users file, etcd service is offline')

        volumes_root = self.dispatcher.call_sync('volume.get_volumes_root')
        if user['home'].startswith(volumes_root):
            group = self.datastore.get_by_id('groups', user['group'])
            if not os.path.exists(user['home']):
                try:
                    self.dispatcher.call_sync('volume.decode_path', user['home'])
                except RpcException as err:
                    raise TaskException(err.code, err.message)
                if (home_before != '/nonexistent' and home_before != user['home']
                   and os.path.exists(home_before)):
                    system('mv', home_before, user['home'])
                else:
                    os.makedirs(user['home'])
                    os.chown(user['home'], user['uid'], group['gid'])
                    os.chmod(user['home'], 0o755)
            elif user['home'] != home_before:
                os.chown(user['home'], user['uid'], group['gid'])
                os.chmod(user['home'], 0o755)
        elif not user['builtin'] and user['home'] not in (None, '/nonexistent'):
            raise TaskException(
                errno.ENOENT,
                "Invalid mountpoint specified for home directory: {0}. ".format(user['home']) +
                "Use '{0}' instead as the mountpoint".format(volumes_root)
            )

        self.dispatcher.dispatch_event('user.changed', {
            'operation': 'update',
            'ids': [user['id']]
        })

    def rollback(self, uid, updated_fields):
        user = self.original_user
        self.datastore.update('users', uid, user)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')


@description("Creates a group")
@accepts(h.all_of(
    h.ref('group'),
    h.required('name'),
    h.forbidden('builtin')
))
class GroupCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating group"

    def describe(self, group):
        return TaskDescription("Adding group {name}", name=group['name'])

    def verify(self, group):
        errors = ValidationException()

        for code, message in check_unixname(group['name']):
            errors.add((0, 'name'), message, code=code)

        if self.datastore.exists('groups', ('name', '=', group['name'])):
            errors.add(
                (0, "name"),
                'Group {0} already exists'.format(group['name']),
                code=errno.EEXIST
            )

        if 'gid' in group and self.datastore.exists('groups', ('gid', '=', group['gid'])):
            errors.add(
                (0, "gid"),
                'Group with GID {0} already exists'.format(group['gid']),
                code=errno.EEXIST
            )

        if errors:
            raise errors

        return ['system']

    def run(self, group):
        if 'gid' not in group:
            # Need to get next free GID
            gid = self.dispatcher.call_sync('group.next_gid')
        else:
            gid = group.pop('gid')

        try:
            group['builtin'] = False
            group['gid'] = gid
            group.setdefault('sudo', False)
            gid = self.datastore.insert('groups', group)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot add group: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot regenerate groups file, etcd service is offline')

        self.dispatcher.dispatch_event('group.changed', {
            'operation': 'create',
            'ids': [gid]
        })

        return gid


@description("Updates a group")
@accepts(
    str,
    h.all_of(
        h.ref('group'),
        h.forbidden('builtin')
    )
)
class GroupUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating group"

    def describe(self, id, updated_fields):
        group = self.datastore.get_by_id('groups', id)
        return TaskDescription("Updating group {name}", name=group['name'] if group else id)

    def verify(self, id, updated_fields):
        errors = ValidationException()

        if 'name' in updated_fields:
            for code, message in check_unixname(updated_fields['name']):
                errors.add((1, 'name'), message, code=code)

            # Check if there is another group with same name being renamed to
            if self.datastore.exists('groups', ('name', '=', updated_fields['name']), ('id', '!=', id)):
                errors.add(
                    (1, "name"),
                    'Group {0} already exists'.format(updated_fields['name']),
                    code=errno.EEXIST
                )

        if errors:
            raise errors

        return ['system']

    def run(self, id, updated_fields):
        try:
            group = self.datastore.get_by_id('groups', id)
            if group is None:
                raise TaskException(errno.ENOENT, 'Group {0} does not exist'.format(id))

            group.update(updated_fields)
            self.datastore.update('groups', id, group)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot update group: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot regenerate groups file, etcd service is offline')

        self.dispatcher.dispatch_event('group.changed', {
            'operation': 'update',
            'ids': [id]
        })


@description("Deletes a group")
@accepts(str)
class GroupDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting group"

    def describe(self, id):
        group = self.datastore.get_by_id('groups', id)
        return TaskDescription("Deleting group {name}", name=group['name'] if group else id)

    def verify(self, id):
        # Check if group exists
        group = self.datastore.get_by_id('groups', id)
        if group is None:
            raise VerifyException(errno.ENOENT, 'Group with given ID does not exist')

        if group['builtin'] is True:
            raise VerifyException(
                errno.EINVAL, 'Group {0} is built-in and can not be deleted'.format(group['name']))

        return ['system']

    def run(self, id):
        try:
            group = self.datastore.get_by_id('groups', id)

            # Remove group from users
            for i in self.datastore.query('users', ('groups', 'in', group['gid'])):
                i['groups'].remove(id)
                self.datastore.update('users', i['id'], i)

            self.datastore.delete('groups', id)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'accounts')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot delete group: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot regenerate config files')

        self.dispatcher.dispatch_event('group.changed', {
            'operation': 'delete',
            'ids': [id]
        })


def collect_debug(dispatcher):
    yield AttachFile('passwd', '/etc/passwd')
    yield AttachFile('group', '/etc/group')


def _init(dispatcher, plugin):
    # Register definitions for objects used
    plugin.register_schema_definition('user', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'uid': {
                'type': 'integer',
                'minimum': 0,
                'maximum': 4294967295
            },
            'builtin': {'type': 'boolean', 'readOnly': True},
            'username': {'type': 'string'},
            'full_name': {'type': ['string', 'null']},
            'email': {
                'oneOf': [
                    {'$ref': 'email'},
                    {'type': 'null'}
                ]
            },
            'locked': {'type': 'boolean'},
            'sudo': {'type': 'boolean'},
            'password_disabled': {'type': 'boolean'},
            'group': {'type': ['string', 'null']},
            'shell': {'type': 'string'},
            'home': {'type': 'string'},
            'password': {'type': ['string', 'null']},
            'unixhash': {'type': ['string', 'null']},
            'smbhash': {'type': ['string', 'null']},
            'sshpubkey': {'type': ['string', 'null']},
            'attributes': {'type': 'object'},
            'groups': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            },
        },
        'additionalProperties': False,
    })

    plugin.register_schema_definition('group', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'gid': {
                'type': 'integer',
                'minimum': 0,
                'maximum': 4294967295
            },
            'builtin': {'type': 'boolean', 'readOnly': True},
            'name': {'type': 'string'},
            'sudo': {'type': 'boolean'},
            'members': {
                'type': 'array',
                'readOnly': True,
                'items': {'type': 'string'}
            }
        },
        'additionalProperties': False,
    })

    # Register provider for querying accounts and groups data
    plugin.register_provider('user', UserProvider)
    plugin.register_provider('group', GroupProvider)

    # Register task handlers
    plugin.register_task_handler('user.create', UserCreateTask)
    plugin.register_task_handler('user.update', UserUpdateTask)
    plugin.register_task_handler('user.delete', UserDeleteTask)
    plugin.register_task_handler('group.create', GroupCreateTask)
    plugin.register_task_handler('group.update', GroupUpdateTask)
    plugin.register_task_handler('group.delete', GroupDeleteTask)

    # Register event types
    plugin.register_event_type('user.changed')
    plugin.register_event_type('group.changed')

    # Register debug hook
    plugin.register_debug_hook(collect_debug)
