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

from datetime import datetime


class ZfsConverter(object):
    def __init__(self, typ, **kwargs):
        self.typ = typ
        self.readonly = kwargs.pop('readonly', False)
        self.nullable = kwargs.pop('nullable', False)
        self.null = kwargs.pop('null', '-')
        self.no = kwargs.pop('no', 'no')
        self.yes = kwargs.pop('yes', 'yes')

    def to_native(self, value):
        if self.nullable and value == self.null:
            return None

        if self.typ is int:
            return int(value)

        if self.typ is str:
            return value

        if self.typ is bool:
            if value == self.yes:
                return True

            if value == self.no:
                return False

            return None

        if self.typ == datetime:
            return datetime.fromtimestamp(int(value))

    def to_property(self, value):
        if self.readonly:
            raise ValueError('Property is read-only')

        if value is None:
            if not self.nullable:
                raise ValueError('Property is not nullable')

            return self.null

        if self.typ is int:
            return str(value)

        if self.typ is str:
            return value

        if self.typ is bool:
            return self.yes if value else self.no

        if self.typ is datetime:
            return str(value.timestamp())
