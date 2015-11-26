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
import logging


logger = logging.getLogger('AlertVolume')
degraded_volumes = []


def _depends():
    return ['AlertPlugin', 'VolumePlugin', 'ZfsPlugin']


def _init(dispatcher, plugin):
    def volume_status(volume):
        if volume['status'] == 'ONLINE' and volume['name'] in degraded_volumes:
            degraded_volumes.remove(volume['name'])
            dispatcher.rpc.call_sync('alert.emit', {
                'name': 'volume.status',
                'description': 'The volume {0} state is {1}'.format(
                    volume['name'],
                    volume['status'],
                ),
                'severity': 'INFO',
            })

        if volume['status'] != 'ONLINE' and volume['name'] not in degraded_volumes:
            degraded_volumes.append(volume['name'])
            dispatcher.rpc.call_sync('alert.emit', {
                'name': 'volume.status',
                'description': 'The volume {0} state is {1}'.format(
                    volume['name'],
                    volume['status'],
                ),
                'severity': 'CRITICAL',
            })

    def volumes_upgraded():
        for volume in dispatcher.rpc.call_sync('volume.query'):
            if volume['status'] == 'UNAVAIL':
                continue

            if volume.get('upgraded') is not False:
                continue

            dispatcher.rpc.call_sync('alert.emit', {
                'name': 'volume.version',
                'description': 'New feature flags are available for volume {0}'.format(volume['name']),
                'severity': 'WARNING',
            })

    dispatcher.call_sync('alert.register_alert', 'volume.status', 'Volume Status')
    dispatcher.call_sync('alert.register_alert', 'volume.version', 'Volume Version')

    for i in dispatcher.call_sync('volume.query'):
        volume_status(i)

    def on_volume_change(args):
        if args['operation'] not in ('create', 'update'):
            return

        for i in args['entities']:
            volume_status(i)

    plugin.register_event_handler('entity-subscriber.volume.changed', on_volume_change)
    volumes_upgraded()
