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

import enum
import io
import os
import errno
import re
import logging
import paramiko
from paramiko import RSAKey, AuthenticationException
from datetime import datetime
from dateutil.parser import parse as parse_datetime
from task import Provider, Task, ProgressTask, VerifyException, TaskException, TaskWarning, query
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from freenas.dispatcher.client import Client
from freenas.utils import to_timedelta, first_or_default
from freenas.utils.query import wrap
from lib import sendzfs

"""
# Bandwidth Limit.
if  bandlim != 0:
    throttle = '/usr/local/bin/throttle -K %d | ' % bandlim
else:
    throttle = ''

#
# Build the SSH command
#

# Cipher
if cipher == 'fast':
    sshcmd = ('/usr/bin/ssh -c arcfour256,arcfour128,blowfish-cbc,'
              'aes128-ctr,aes192-ctr,aes256-ctr -i /data/ssh/replication'
              ' -o BatchMode=yes -o StrictHostKeyChecking=yes'
              # There's nothing magical about ConnectTimeout, it's an average
              # of wiliam and josh's thoughts on a Wednesday morning.
              # It will prevent hunging in the status of "Sending".
              ' -o ConnectTimeout=7'
             )
elif cipher == 'disabled':
    sshcmd = ('/usr/bin/ssh -ononeenabled=yes -ononeswitch=yes -i /data/ssh/replication -o BatchMode=yes'
              ' -o StrictHostKeyChecking=yes'
              ' -o ConnectTimeout=7')
else:
    sshcmd = ('/usr/bin/ssh -i /data/ssh/replication -o BatchMode=yes'
              ' -o StrictHostKeyChecking=yes'
              ' -o ConnectTimeout=7')

# Remote IP/hostname and port.  This concludes the preparation task to build SSH command
sshcmd = '%s -p %d %s' % (sshcmd, remote_port, remote)
"""

logger = logging.getLogger(__name__)
SYSTEM_RE = re.compile('^[^/]+/.system.*')
AUTOSNAP_RE = re.compile(
    '^(?P<prefix>\w+)-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})'
    '.(?P<hour>\d{2})(?P<minute>\d{2})-(?P<lifetime>\d+[hdwmy])(-(?P<sequence>\d+))?$'
)
SSH_OPTIONS = {
    'NONE': [
        '-ononeenabled=yes',
        '-ononeswitch=yes',
        '-o BatchMode=yes',
        '-o ConnectTimeout=7'
    ],
    'FAST': [
        '-c arcfour256,arcfour128,blowfish-cbc,aes128-ctr,aes192-ctr,aes256-ctr',
        '-o BatchMode=yes',
        '-o ConnectTimeout=7'
    ],
    'NORMAL': [
        '-o BatchMode=yes',
        '-o ConnectTimeout=7'
    ]
}


class ReplicationActionType(enum.Enum):
    SEND_STREAM = 1
    DELETE_SNAPSHOTS = 2
    DELETE_DATASET = 3


class ReplicationAction(object):
    def __init__(self, type, localfs, remotefs, **kwargs):
        self.type = type
        self.localfs = localfs
        self.remotefs = remotefs
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getstate__(self):
        d = dict(self.__dict__)
        d['type'] = d['type'].name
        return d


class ReplicationProvider(Provider):
    def get_public_key(self):
        return self.configstore.get('replication.key.public')

    def scan_keys_on_host(self, hostname):
        return self.dispatcher.call_task_sync('replication.scan_hostkey', hostname)

    def datasets_from_link(self, link):
        datasets = []
        for dataset in link['datasets']:
            if link['recursive']:
                try:
                    dependencies = self.dispatcher.call_sync(
                        'zfs.dataset.query',
                        [('name', '~', '^{0}(/|$)'.format(dataset))],
                    )
                except RpcException:
                    raise RpcException(errno.ENOENT, 'Dataset {0} not found'.format(dataset))

                for dependency in dependencies:
                    if dependency not in datasets:
                        datasets.append(dependency)
            else:
                dependency = self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', dataset)], {'single': True})
                if dependency:
                    if dependency not in datasets:
                        datasets.append(dependency)
                else:
                    raise RpcException(errno.ENOENT, 'Dataset {0} not found'.format(dataset))

        return sorted(datasets, key=lambda d: d['name'])


class ReplicationLinkProvider(Provider):
    @query('replication-link')
    def query(self, filter=None, params=None):
        return self.datastore.query(
            'replication.links', *(filter or []), **(params or {})
        )

    def get_one(self, name):
        if self.datastore.exists('replication.links', ('name', '=', name)):
            return self.datastore.get_one('replication.links', ('name', '=', name))
        else:
            return None


@accepts(str)
class ScanHostKeyTask(Task):
    def verify(self, hostname):
        return []

    def run(self, hostname):
        transport = paramiko.transport.Transport(hostname)
        transport.start_client()
        key = transport.get_remote_server_key()
        return {
            'name': key.get_name(),
            'key': key.get_base64()
        }


@description("Sets up a replication link")
@accepts(h.all_of(
        h.ref('replication-link'),
        h.required('name', 'partners', 'master', 'datasets', 'replicate_services', 'bidirectional', 'recursive')
    )
)
class ReplicationCreate(Task):
    def verify(self, link):
        partners = link['partners']
        name = link['name']
        ip_matches = False

        ips = self.dispatcher.call_sync('network.config.get_my_ips')
        for ip in ips:
            for partner in partners:
                if '@' not in partner:
                    raise VerifyException(
                        errno.EINVAL,
                        'Please provide replication link partners as username@host'
                    )
                if partner.endswith(ip):
                    ip_matches = True

        if not ip_matches:
            raise VerifyException(errno.EINVAL, 'Provided partner IPs do not create a valid pair. Check addresses.')

        if self.datastore.exists('replication.links', ('name', '=', name)):
            raise VerifyException(errno.EEXIST, 'Replication link with name {0} already exists'.format(name))

        if len(partners) != 2:
            raise VerifyException(
                errno.EINVAL,
                'Replication link can only have 2 partners. Value {0} is not permitted.'.format(len(partners))
            )

        usernames = [partners[0].split('@')[0], partners[1].split('@')[0]]
        if self.dispatcher.call_sync('user.query', [('username', '=', usernames[0])], {'single': True}):
            pass
        elif not self.dispatcher.call_sync('user.query', [('username', '=', usernames[1])], {'single': True}):
            raise VerifyException(
                errno.ENOENT,
                'At least one of provided user names is not valid: {0}, {1}'.format(usernames[0], usernames[1])
            )

        if link['master'] not in partners:
            raise VerifyException(
                errno.EINVAL,
                'Replication master must be one of replication partners {0}, {1}'.format(partners[0], partners[1])
            )

        if link['replicate_services'] and not link['bidirectional']:
            raise VerifyException(
                errno.EINVAL,
                'Replication of services is available only when bi-directional replication is selected'
            )

        return []

    def run(self, link):
        link['id'] = link['name']
        if 'update_date' not in link:
            link['update_date'] = str(datetime.utcnow())

        is_master, remote = get_replication_state(self.dispatcher, link)
        remote_client = get_remote_client(remote)

        if is_master:
            with self.dispatcher.get_lock('volumes'):
                remote_link = remote_client.call_sync('replication.link.get_one', link['name'])
                if remote_link:
                    if remote_link != link:
                        raise TaskException(
                            errno.EEXIST,
                            'Replication link {0} already exists on {1}'.format(link['name'], remote)
                        )

                datasets_to_replicate = self.dispatcher.call_sync('replication.datasets_from_link', link)
                root = self.dispatcher.call_sync('volume.get_volumes_root')

                for dataset in datasets_to_replicate:
                    if link['replicate_services']:
                        for share in self.dispatcher.call_sync('share.get_dependencies', os.path.join(root, dataset), False, False):
                            remote_share = remote_client.call_sync(
                                'share.query',
                                [('name', '=', share['name'])],
                                {'single': True}
                            )
                            if remote_share:
                                raise TaskException(
                                    errno.EEXIST,
                                    'Share {0} already exists on {1}'.format(share['name'], remote.split('@', 1)[1])
                                )

                        container = self.dispatcher.call_sync('container.get_dependent', dataset)
                        if container:
                            remote_container = remote_client.call_sync(
                                'container.query',
                                [('name', '=', container['name'])],
                                {'single': True}
                            )
                            if remote_container:
                                raise TaskException(
                                    errno.EEXIST,
                                    'Container {0} already exists on {1}'.format(container['name'], remote.split('@', 1)[1])
                                )

                    split_dataset = dataset.split('/', 1)
                    volume_name = split_dataset[0]
                    dataset_name = None
                    if len(split_dataset) == 2:
                        dataset_name = split_dataset[1]

                    vol = self.datastore.get_one('volumes', ('id', '=', volume_name))
                    if vol.get('encrypted'):
                        raise TaskException(
                            errno.EINVAL,
                            'Encrypted volumes cannot be included in replication links: {0}'.format(volume_name)
                        )

                    remote_volume = remote_client.call_sync(
                        'volume.query',
                        [('id', '=', volume_name)],
                        {'single': True}
                    )
                    if remote_volume:
                        if remote_volume.get('encrypted'):
                            raise TaskException(
                                errno.EINVAL,
                                'Encrypted volumes cannot be included in replication links: {0}'.format(volume_name)
                            )

                    remote_dataset = remote_client.call_sync(
                        'zfs.dataset.query',
                        [('name', '=', dataset)],
                        {'single': True}
                    )

                    if not remote_dataset:
                        if not remote_volume:
                            try:
                                remote_client.call_task_sync(
                                    'volume.create',
                                    {
                                        'id': vol['id'],
                                        'type': vol['type'],
                                        'params': {'encryption': False},
                                        'topology': vol['topology']
                                    }
                                )
                            except RpcException as e:
                                raise TaskException(
                                    e.code,
                                    'Cannot create exact duplicate of {0} on {1}. Message: {2}'.format(
                                        volume_name,
                                        remote,
                                        e.message
                                    ),
                                    stacktrace=e.stacktrace
                                )

                        if dataset_name:
                            for sub_dataset in dataset_name.split('/'):
                                sub_dataset_name = dataset[:dataset.index(sub_dataset) - 1]
                                remote_sub_dataset = remote_client.call_sync(
                                    'zfs.dataset.query',
                                    [('name', '=', sub_dataset_name)],
                                    {'single': True}
                                )
                                if not remote_sub_dataset:
                                    local_sub_dataset = self.dispatcher.call_sync(
                                        'zfs.dataset.query',
                                        [('name', '=', sub_dataset_name)],
                                        {'single': True}
                                    )
                                    try:
                                        dataset_properties = {
                                            'id': local_sub_dataset['name'],
                                            'volume': local_sub_dataset['pool']
                                        }
                                        if local_sub_dataset['mountpoint']:
                                            dataset_properties['mountpoint'] = local_sub_dataset['mountpoint']
                                        remote_client.call_task_sync(
                                            'volume.dataset.create',
                                            dataset_properties
                                        )
                                    except RpcException as e:
                                        raise TaskException(
                                            e.code,
                                            'Cannot create exact duplicate of {0} on {1}. Message: {2}'.format(
                                                sub_dataset_name,
                                                remote,
                                                e.message
                                            ),
                                            stacktrace=e.stacktrace
                                        )

                id = self.datastore.insert('replication.links', link)
                remote_client.call_task_sync('replication.create', link)

                set_replicated_datasets_enabled(remote_client, False, datasets_to_replicate, False)
        else:
            remote_link = remote_client.call_sync('replication.link.get_one', link['name'])
            if not remote_link:
                remote_client.call_task_sync('replication.create', link)
            else:
                if remote_link != link:
                    raise TaskException(
                        errno.EEXIST,
                        'Replication link {0} already exists on {1}'.format(link['name'], remote)
                    )
            id = self.datastore.insert('replication.links', link)

        self.dispatcher.dispatch_event('replication.changed', {
            'operation': 'create',
            'ids': [id]
        })
        remote_client.disconnect()


@description("Deletes replication link")
@accepts(str)
class ReplicationDelete(Task):
    def verify(self, name):
        if not self.datastore.exists('replication.links', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Replication link {0} do not exist.'.format(name))

        return []

    def run(self, name):
        link = get_latest_replication_link(self.dispatcher, self.datastore, name)
        is_master, remote = get_replication_state(self.dispatcher, link)

        self.datastore.delete('replication.links', link['id'])

        remote_client = get_remote_client(remote)
        if remote_client.call_sync('replication.link.get_one', name):
            remote_client.call_task_sync('replication.delete', name)
        remote_client.disconnect()

        self.dispatcher.dispatch_event('replication.changed', {
            'operation': 'delete',
            'ids': [link['id']]
        })


@description("Switch state of bi-directional replication link")
@accepts(str, h.ref('replication-link'))
class ReplicationUpdate(Task):
    def verify(self, name, updated_fields):
        if not self.datastore.exists('replication.links', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Bi-directional replication link {0} do not exist.'.format(name))

        return []

    def run(self, name, updated_fields):
        link = get_latest_replication_link(self.dispatcher, self.datastore, name)
        is_master, remote = get_replication_state(self.dispatcher, link)
        if 'update_date' not in updated_fields:
            link['update_date'] = str(datetime.utcnow())

        for partner in link['partners']:
            if partner != link['master']:
                link['master'] = partner

        self.datastore.update('replication.links', link['id'], link)

        self.dispatcher.dispatch_event('replication.changed', {
            'operation': 'update',
            'ids': [link['id']]
        })

        remote_client = get_remote_client(remote)
        if link is not remote_client.call_sync('replication.link.get_one', name):
            remote_client.call_task_sync(
                'replication.update',
                link['name'],
                updated_fields
            )
        remote_client.disconnect()

        is_master, remote = get_replication_state(self.dispatcher, link)
        set_replicated_datasets_enabled(self.dispatcher, is_master, link['datasets'], True)


@description("Triggers replication in bi-directional replication")
@accepts(str)
class ReplicationSync(Task):
    def verify(self, name):
        if not self.datastore.exists('replication.links', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Bi-directional replication link {0} do not exist.'.format(name))

        link = self.datastore.get_one('replication.links', ('name', '=', name))

        return ['zpool:{0}'.format(p) for p in link['datasets']]

    def run(self, name):
        link = get_latest_replication_link(self.dispatcher, self.datastore, name)
        is_master, remote = get_replication_state(self.dispatcher, link)
        remote_client = get_remote_client(remote)
        if is_master:
            with self.dispatcher.get_lock('volumes'):
                set_replicated_datasets_enabled(remote_client, True, link['datasets'], False)
                for volume in link['datasets']:
                    self.join_subtasks(self.run_subtask(
                        'replication.replicate_dataset',
                        volume,
                        volume,
                        {
                            'remote': remote,
                            'remote_dataset': volume,
                            'recursive': True
                        }
                    ))

                    if link['replicate_services']:
                        remote_client.call_task_sync('volume.autoimport', volume, 'containers')
                        remote_client.call_task_sync('volume.autoimport', volume, 'shares')

                set_replicated_datasets_enabled(remote_client, False, link['datasets'], True)
        else:
            remote_client.call_task_sync(
                'replication.sync',
                link['name']
            )

        remote_client.disconnect()
        self.dispatcher.dispatch_event('replication.changed', {
            'operation': 'update',
            'ids': [link['id']]
        })


@accepts(str, str, bool, str, str, bool)
@returns(str)
class SnapshotDatasetTask(Task):
    def verify(self, pool, dataset, recursive, lifetime, prefix='auto', replicable=False):
        if not self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', dataset)], {'single': True}):
            raise VerifyException(errno.ENOENT, 'Dataset {0} not found'.format(dataset))

        return ['zfs:{0}'.format(dataset)]

    def run(self, pool, dataset, recursive, lifetime, prefix='auto', replicable=False):
        def is_expired(snapshot):
            match = AUTOSNAP_RE.match(snapshot['snapshot_name'])
            if not match:
                return False

            if snapshot['holds']:
                return False

            if match.group('prefix') != prefix:
                return False

            delta = to_timedelta(match.group('lifetime'))
            creation = parse_datetime(snapshot['properties.creation.value'])
            return creation + delta < datetime.utcnow()

        snapshots = list(filter(is_expired, wrap(self.dispatcher.call_sync('zfs.dataset.get_snapshots', dataset))))
        snapname = '{0}-{1:%Y%m%d.%H%M}-{2}'.format(prefix, datetime.utcnow(), lifetime)
        params = {'org.freenas:replicate': {'value': 'yes'}} if replicable else None
        base_snapname = snapname

        # Pick another name in case snapshot already exists
        for i in range(1, 99):
            if self.dispatcher.call_sync(
                'zfs.snapshot.query',
                [('name', '=', '{0}@{1}'.format(dataset, snapname))],
                {'count': True}
            ):
                snapname = '{0}-{1}'.format(base_snapname, i)
                continue

            break

        self.join_subtasks(
            self.run_subtask('zfs.create_snapshot', pool, dataset, snapname, recursive, params),
            self.run_subtask(
                'zfs.delete_multiple_snapshots',
                pool,
                dataset,
                list(map(lambda s: s['snapshot_name'], snapshots)),
                True
            )
        )


@description("Runs a replication task with the specified arguments")
#@accepts(h.all_of(
#    h.ref('autorepl'),
#    h.required(
#        'remote',
#        'remote_port',
#        'dedicateduser',
#        'cipher',
#        'localfs',
#        'remotefs',
#        'compression',
#        'bandlim',
#        'followdelete',
#        'recursive',
#    ),
#))
class ReplicateDatasetTask(ProgressTask):
    def verify(self, pool, localds, options, dry_run=False):
        return ['zfs:{0}'.format(localds)]

    def run(self, pool, localds, options, dry_run=False):
        remote = options['remote']
        remoteds = options['remote_dataset']
        followdelete = options.get('followdelete', False)
        recursive = options.get('recursive', False)
        lifetime = options.get('lifetime', '1y')

        self.join_subtasks(self.run_subtask(
            'volume.snapshot_dataset',
            pool,
            localds,
            True,
            lifetime,
            'repl',
            True
        ))

        datasets = [localds]
        remote_datasets = []
        actions = []

        remote_client = get_remote_client(options['remote'])

        def is_replicated(snapshot):
            if snapshot.get('properties.org\\.freenas:replicate.value') != 'yes':
                # Snapshot is not subject to replication
                return False

            return True

        def matches(pair):
            src, tgt = pair
            srcsnap = src['snapshot_name']
            tgtsnap = tgt['snapshot_name']
            return srcsnap == tgtsnap and src['properties.creation.rawvalue'] == tgt['properties.creation.rawvalue']

        if recursive:
            datasets = self.dispatcher.call_sync(
                'zfs.dataset.query',
                [('name', '~', '^{0}(/|$)'.format(localds))],
                {'select': 'name'}
            )

            remote_datasets = remote_client.call_sync(
                'zfs.dataset.query',
                [('name', '~', '^{0}(/|$)'.format(remoteds))],
                {'select': 'name'}
            )

        self.set_progress(0, 'Reading replication state from remote side...')

        for ds in datasets:
            localfs = ds
            remotefs = localfs.replace(localds, remoteds, 1)
            local_snapshots = list(filter(
                is_replicated,
                wrap(self.dispatcher.call_sync('zfs.dataset.get_snapshots', localfs))
            ))

            try:
                remote_snapshots_full = wrap(
                    remote_client.call_sync(
                        'zfs.dataset.get_snapshots',
                        remotefs
                    )
                )
                remote_snapshots = list(filter(is_replicated, remote_snapshots_full))
            except RpcException as err:
                if err.code == errno.ENOENT:
                    # Dataset not found on remote side
                    remote_snapshots_full = None
                else:
                    raise TaskException(err.code, 'Cannot contact {0}: {1}'.format(remote, err.message))

            snapshots = local_snapshots[:]
            found = None

            if remote_snapshots_full:
                # Find out the last common snapshot.
                pairs = list(filter(matches, zip(local_snapshots, remote_snapshots)))
                if pairs:
                    pairs.sort(key=lambda p: int(p[0]['properties.creation.rawvalue']), reverse=True)
                    found, _ = first_or_default(None, pairs)

                if found:
                    if followdelete:
                        delete = []
                        for snap in remote_snapshots:
                            rsnap = snap['snapshot_name']
                            if not first_or_default(lambda s: s['snapshot_name'] == rsnap, local_snapshots):
                                delete.append(rsnap)

                        actions.append(ReplicationAction(
                            ReplicationActionType.DELETE_SNAPSHOTS,
                            localfs,
                            remotefs,
                            snapshots=delete
                        ))

                    index = local_snapshots.index(found)

                    for idx in range(index + 1, len(local_snapshots)):
                        actions.append(ReplicationAction(
                            ReplicationActionType.SEND_STREAM,
                            localfs,
                            remotefs,
                            incremental=True,
                            anchor=local_snapshots[idx - 1]['snapshot_name'],
                            snapshot=local_snapshots[idx]['snapshot_name']
                        ))
                else:
                    actions.append(ReplicationAction(
                        ReplicationActionType.DELETE_SNAPSHOTS,
                        localfs,
                        remotefs,
                        snapshots=map(lambda s: s['snapshot_name'], remote_snapshots_full)
                    ))

                    for idx in range(0, len(snapshots)):
                        actions.append(ReplicationAction(
                            ReplicationActionType.SEND_STREAM,
                            localfs,
                            remotefs,
                            incremental=idx > 0,
                            anchor=snapshots[idx - 1]['snapshot_name'] if idx > 0 else None,
                            snapshot=snapshots[idx]['snapshot_name']
                        ))
            else:
                logger.info('New dataset {0} -> {1}:{2}'.format(localfs, remote, remotefs))
                for idx in range(0, len(snapshots)):
                    actions.append(ReplicationAction(
                        ReplicationActionType.SEND_STREAM,
                        localfs,
                        remotefs,
                        incremental=idx > 0,
                        anchor=snapshots[idx - 1]['snapshot_name'] if idx > 0 else None,
                        snapshot=snapshots[idx]['snapshot_name']
                    ))

        for rds in remote_datasets:
            remotefs = rds
            localfs = remotefs.replace(remoteds, localds, 1)

            if localfs not in datasets:
                actions.append(ReplicationAction(
                    ReplicationActionType.DELETE_DATASET,
                    localfs,
                    remotefs
                ))

        # 1st pass - estimate send size
        self.set_progress(0, 'Estimating send size...')
        total_send_size = 0
        done_send_size = 0

        for action in actions:
            if action.type == ReplicationActionType.SEND_STREAM:
                size = self.dispatcher.call_sync(
                    'zfs.dataset.estimate_send_size',
                    action.localfs,
                    action.snapshot,
                    getattr(action, 'anchor', None)
                )

                action.send_size = size
                total_send_size += size

        if dry_run:
            return actions

        # 2nd pass - actual send
        for idx, action in enumerate(actions):
            progress = float(idx) / len(actions) * 100

            if action.type == ReplicationActionType.DELETE_SNAPSHOTS:
                self.set_progress(progress, 'Removing snapshots on remote dataset {0}'.format(action.remotefs))
                # Remove snapshots on remote side
                result = remote_client.call_task_sync(
                    'zfs.delete_multiple_snapshots',
                    action.remotefs.split('/')[0],
                    action.remotefs,
                    list(action.snapshots)
                )

                if result['state'] != 'FINISHED':
                    raise TaskException(errno.EFAULT, 'Failed to destroy snapshots on remote end: {0}'.format(
                        result['error']['message']
                    ))

            if action.type == ReplicationActionType.SEND_STREAM:
                self.set_progress(progress, 'Sending {0} stream of snapshot {1}/{2}'.format(
                    'incremental' if action.incremental else 'full',
                    action.localfs,
                    action.snapshot
                ))

                if not action.incremental:
                    send_dataset(remote, options.get('remote_hostkey'), None, action.snapshot, action.localfs, action.remotefs, '', 0)
                else:
                    send_dataset(remote, options.get('remote_hostkey'), action.anchor, action.snapshot, action.localfs, action.remotefs, '', 0)

            if action.type == ReplicationActionType.DELETE_DATASET:
                self.set_progress(progress, 'Removing remote dataset {0}'.format(action.remotefs))
                result = remote_client.call_task_sync(
                    'zfs.destroy',
                    action.remotefs.split('/')[0],
                    action.remotefs
                )

                if result['state'] != 'FINISHED':
                    raise TaskException(errno.EFAULT, 'Failed to destroy dataset {0} on remote end: {1}'.format(
                        action.remotefs,
                        result['error']['message']
                    ))

        remote_client.disconnect()

        return actions


#
# Attempt to send a snapshot or increamental stream to remote.
#
def send_dataset(remote, hostkey, fromsnap, tosnap, dataset, remotefs, compression, throttle):
    zfs = sendzfs.SendZFS()
    zfs.send(remote, hostkey, fromsnap, tosnap, dataset, remotefs, compression, throttle, 1024*1024, None)


def get_remote_client(remote):
    with open('/etc/replication/key') as f:
        pkey = RSAKey.from_private_key(f)

    try:
        remote_client = Client()
        remote_client.connect('ws+ssh://{0}'.format(remote), pkey=pkey)
        remote_client.login_service('replicator')
        return remote_client

    except AuthenticationException:
        raise RpcException(errno.EAUTH, 'Cannot connect to {0}'.format(remote))


def get_latest_replication_link(dispatcher, datastore, name):
    if datastore.exists('replication.links', ('name', '=', name)):
        local_link = datastore.get_one('replication.links', ('name', '=', name))
        ips = dispatcher.call_sync('network.config.get_my_ips')
        remote = ''
        client = None
        for partner in local_link['partners']:
            if partner.split('@', 1)[1] not in ips:
                remote = partner

        try:
            client = get_remote_client(remote)
            remote_link = client.call_sync('replication.link.get_one', name)
            client.disconnect()
            if not remote_link:
                return local_link

            if parse_datetime(local_link['update_date']) > parse_datetime(remote_link['update_date']):
                return local_link
            else:
                return remote_link

        except RpcException:
            if client:
                client.disconnect()
            return local_link

    else:
        return None


def get_replication_state(dispatcher, link):
    is_master = False
    remote = ''
    ips = dispatcher.call_sync('network.config.get_my_ips')
    for ip in ips:
        for partner in link['partners']:
            if partner.endswith(ip) and partner == link['master']:
                is_master = True
    for partner in link['partners']:
        if partner.split('@', 1)[1] not in ips:
            remote = partner

    return is_master, remote


def set_replicated_datasets_enabled(client, enabled, datasets, set_services):
    for volume in datasets:
        if set_services:
            client.call_task_sync(
                'zfs.update',
                volume, volume,
                {'readonly': {'value': 'off'}}
            )
            vol_path = client.call_sync('volume.get_dataset_path', volume)
            vol_shares = client.call_sync('share.get_dependencies', vol_path, False)
            vol_containers = client.call_sync('container.query', [('target', '=', volume)])
            for share in vol_shares:
                client.call_task_sync('share.update', share['id'], {'enabled': enabled})
            for container in vol_containers:
                client.call_task_sync('container.update', container['id'], {'enabled': enabled})
        client.call_task_sync(
            'zfs.update',
            volume, volume,
            {'readonly': {'value': 'off' if enabled else 'on'}}
        )


def _init(dispatcher, plugin):
    plugin.register_schema_definition('replication', {
        'type': 'object',
        'properties': {
            'remote': {'type': 'string'},
            'remote_port': {'type': 'string'},
            'remote_hostkey': {'type': 'string'},
            'remote_dataset': {'type': 'string'},
            'cipher': {
                'type': 'string',
                'enum': ['NORMAL', 'FAST', 'DISABLED']
            },
            'compression': {
                'type': 'string',
                'enum': ['none', 'pigz', 'plzip', 'lz4', 'xz']
            },
            'bandwidth_limit': {'type': 'string'},
            'followdelete': {'type': 'boolean'},
            'recursive': {'type': 'boolean'},
        },
        'additionalProperties': False,
    })

    plugin.register_schema_definition('replication-link', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'partners': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'master': {'type': 'string'},
            'update_date': {'type': 'string'},
            'datasets': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'bidirectional': {'type': 'boolean'},
            'replicate_services': {'type': 'boolean'},
            'recursive': {'type': 'boolean'}
        },
        'additionalProperties': False,
    })

    plugin.register_provider('replication', ReplicationProvider)
    plugin.register_provider('replication.link', ReplicationLinkProvider)
    plugin.register_task_handler('volume.snapshot_dataset', SnapshotDatasetTask)
    plugin.register_task_handler('replication.scan_hostkey', ScanHostKeyTask)
    plugin.register_task_handler('replication.replicate_dataset', ReplicateDatasetTask)
    plugin.register_task_handler('replication.create', ReplicationCreate)
    plugin.register_task_handler('replication.update', ReplicationUpdate)
    plugin.register_task_handler('replication.sync', ReplicationSync)
    plugin.register_task_handler('replication.delete', ReplicationDelete)

    plugin.register_event_type('replication.changed')

    # Generate replication key pair on first run
    if not dispatcher.configstore.get('replication.key.private') or not dispatcher.configstore.get('replication.key.public'):
        key = RSAKey.generate(bits=2048)
        buffer = io.StringIO()
        key.write_private_key(buffer)
        dispatcher.configstore.set('replication.key.private', buffer.getvalue())
        dispatcher.configstore.set('replication.key.public', key.get_base64())

    def on_etcd_resume(args):
        if args.get('name') != 'etcd.generation':
            return

        dispatcher.call_sync('etcd.generation.generate_group', 'replication')

    plugin.register_event_handler('plugin.service_resume', on_etcd_resume)
