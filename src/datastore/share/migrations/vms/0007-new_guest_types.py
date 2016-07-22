#+
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

migration_dict = {
    'boot2docker': 'linux64',
    'centos': 'linux64',
    'debian': 'linux64',
    'fedora': 'linux64',
    'freebsd': 'freebsd64',
    'freenas': 'freebsd64',
    'netbsd': 'netbsd64',
    'openbsd': 'openbsd64',
    'opensuse': 'linux64',
    'pfsense': 'freebsd64',
    'solaris': 'solaris64,',
    'ubuntu': 'linux64'
}


def probe(obj, ds):
    known_os_list = [
            'linux64',
            'freebsd32',
            'freebsd64',
            'netbsd64',
            'openbsd32',
            'openbsd64',
            'windows64'
            'solaris64',
            'other',
        ]
    return ('guest_type' not in obj) or (obj['guest_type'] not in known_os_list)


def apply(obj, ds):
    obj['guest_type'] = migration_dict.get(obj.get('guest_type', 'other'), 'other')
    return obj
