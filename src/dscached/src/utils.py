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

import dns.resolver
import dns.exception
import krb5
from ldap3.utils.conv import escape_filter_chars


class LdapQueryBuilder(object):
    def __init__(self, mappings):
        self.mappings = mappings

    def _predicate(self, *args):
        if len(args) == 2:
            return self._joint_predicate(*args)

        if len(args) == 3:
            return self._operator_predicate(*args)

    def _operator_predicate(self, name, op, value):
        if op == '=':
            return '({0}={1})'.format(self.mappings[name], escape_filter_chars(value))

        if op == '!=':
            return '(!{0}={1})'.format(self.mappings[name], escape_filter_chars(value))

    def _joint_predicate(self, op, value):
        if op == 'or':
            return '(|{0})'.format(''.join(self._predicate(i) for i in value))

        if op == 'and':
            return '(&{0})'.format(''.join(self._predicate(i) for i in value))

    def build_query(self, params):
        if len(params) == 1:
            return self._predicate(params[0])

        return self._joint_predicate('and', params)


def join_dn(*parts):
    return ','.join(parts)


def domain_to_dn(domain):
    return ','.join('dc={0}'.format(i) for i in domain.split('.'))


def obtain_or_renew_ticket(principal, password, renew_life=None):
    ctx = krb5.Context()
    cc = krb5.CredentialsCache(ctx)

    tgt = ctx.obtain_tgt_password(principal, password, renew_life=renew_life)
    cc.add(tgt)


def have_ticket(principal):
    ctx = krb5.Context()
    cc = krb5.CredentialsCache(ctx)

    for i in cc.entries:
        if i.client == principal:
            return True

    return False


def get_srv_records(service, protocol, domain):
    try:
        answer = dns.resolver.query('_{0}._{1}.{2}'.format(service, protocol, domain), dns.rdatatype.SRV)
        for i in answer:
            yield i.target
    except dns.exception.DNSException:
        return
