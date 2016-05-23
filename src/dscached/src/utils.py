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

import krb5
from freenas.utils import first_or_default


def join_dn(*dns):
    return ','.join(dns)


def dn_to_domain(dn):
    return '.'.join(name for typ, name, sep in ldap3.utils.dn.parse_dn(dn))


def domain_to_dn(domain):
    return ','.join('dc={0}'.format(i) for i in domain.split('.'))


def obtain_or_renew_ticket(principal, password, cache=None, service=None, renew_life=None):
    if not service:
        _, realm = principal.split('@')
        service = 'krbtgt/{0}@{0}'.format(realm)

    krb = krb5.Context()
    ccache = krb5.CredentialsCache(krb, cache)
    ticket = first_or_default(lambda e: e.client == principal and e.server == service, ccache.entries)

    if ticket:
        # renew
        if not ticket.expired:
            return

        if ticket.renew_possible:
            tgt = krb.renew_tgt(principal, ccache, service)
            cache.ad(tgt)
            return

    # obtain new
    tgt = krb.obtain_tgt_password(principal, password, service=service, renew_life=renew_life)
    ccache.add(tgt)
    return tgt
