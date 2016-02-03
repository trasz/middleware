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

import os
from freenas.dispatcher.jsonenc import dumps, loads


def first_or_default(f, iterable, default=None):
    i = list(filter(f, iterable))
    if i:
        return i[0]

    return default


def split_dataset(dataset_path):
    pool = dataset_path.split('/')[0]
    return pool, dataset_path


def save_config(conf_path, name_mod, entry):
    with open(os.path.join(conf_path, '.config-{0}.json'.format(name_mod)), 'w', encoding='utf-8') as conf_file:
        conf_file.write(dumps(entry))


def load_config(conf_path, name_mod):
    with open(os.path.join(conf_path, '.config-{0}.json'.format(name_mod)), 'r', encoding='utf-8') as conf_file:
        return loads(conf_file.read())


def delete_config(conf_path, name_mod):
    os.remove(os.path.join(conf_path, '.config-{0}.json'.format(name_mod)))
