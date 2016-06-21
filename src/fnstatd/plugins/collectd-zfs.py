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

import collectd
import libzfs


zfs = None


def init():
    global zfs
    zfs = libzfs.ZFS()


def read():
    for pool in zfs.pools:
        root_vdev_stats = pool.root_vdev.stats.__getstate__()
        for stat in root_vdev_stats:
            if isinstance(root_vdev_stats[stat], list):
                for zio_type in libzfs.ZIOType:
                    if zio_type.value >= len(root_vdev_stats[stat]):
                        break

                    zio_name = zio_type.name
                    value = root_vdev_stats[stat][zio_type.value]
                    dispatch_value(pool.name, stat + '_' + zio_name, value)

                    if stat == 'bytes':
                        dispatch_value(pool.name, 'bandwidth' + '_' + zio_name, value, 'derive')
            else:
                dispatch_value(pool.name, stat, root_vdev_stats[stat])


def dispatch_value(name, instance, value, data_type='gauge'):
    val = collectd.Values()
    val.plugin = 'zfs'
    val.plugin_instance = name
    val.type = data_type
    val.type_instance = instance
    val.values = [value, ]
    val.meta = {'0': True}
    val.dispatch()


collectd.register_init(init)
collectd.register_read(read)
