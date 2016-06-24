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

import errno
import os
import re
import time
import datetime
from pytz import UTC
from datastore import DatastoreException
from freenas.dispatcher.rpc import RpcException, description, accepts
from freenas.dispatcher.rpc import SchemaHelper as h
from task import Provider, Task, TaskException, ValidationException, VerifyException, query, TaskDescription

from OpenSSL import crypto


def export_privatekey(buf, passphrase=None):
    key = crypto.load_privatekey(
        crypto.FILETYPE_PEM,
        buf,
        passphrase=str(passphrase) if passphrase else None
    )

    return crypto.dump_privatekey(
        crypto.FILETYPE_PEM,
        key,
        passphrase=str(passphrase) if passphrase else None
    ).decode('utf-8')


def load_certificate(buf):
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, buf)

    cert_info = {
        'country': cert.get_subject().C,
        'state': cert.get_subject().ST,
        'city': cert.get_subject().L,
        'organization': cert.get_subject().O,
        'common': cert.get_subject().CN,
        'email': cert.get_subject().emailAddress
    }

    signature_algorithm = cert.get_signature_algorithm().decode('utf-8')
    m = re.match('^(.+)[Ww]ith', signature_algorithm)
    if m:
        cert_info['digest_algorithm'] = m.group(1).upper()

    return cert_info


def load_privatekey(buf, passphrase=None):
    return crypto.load_privatekey(
        crypto.FILETYPE_PEM,
        buf,
        passphrase=lambda x: str(passphrase) if passphrase else ''
    )


@description("Provider for certificates")
class CertificateProvider(Provider):
    @query('crypto-certificate')
    def query(self, filter=None, params=None):
        def extend(certificate):
            buf = certificate.get('csr' if certificate['type'] == 'CERT_CSR' else 'certificate')
            if buf:
                if certificate['type'] == 'CERT_CSR':
                    cert = crypto.load_certificate_request(crypto.FILETYPE_PEM, buf)
                else:
                    cert = crypto.load_certificate(crypto.FILETYPE_PEM, buf)
                certificate['dn'] = '/{0}'.format('/'.join([
                    '{0}={1}'.format(c[0], c[1]) for c in cert.get_subject().get_components()
                ]))

                try:
                    certificate['valid_from'] = cert.get_notBefore()
                    certificate['valid_until'] = cert.get_notAfter()
                except Exception:
                    certificate['valid_from'] = None
                    certificate['valid_until'] = None

            if certificate['type'].startswith('CA_'):
                cert_path = '/etc/certificates/CA'
            else:
                cert_path = '/etc/certificates'

            if certificate.get('certificate'):
                certificate['certificate_path'] = os.path.join(
                    cert_path, '{0}.crt'.format(certificate['name']))
                # Load and dump private key to make sure its in desired format
                # This is code ported from 9.3 and must be reviewed as it may very well be useless
                cert = crypto.load_certificate(crypto.FILETYPE_PEM, certificate['certificate'])
                certificate['certificate'] = crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode('utf-8')

            if certificate.get('privatekey'):
                certificate['privatekey_path'] = os.path.join(
                    cert_path, '{0}.key'.format(certificate['name']))
                # Load and dump private key to make sure its in desired format
                # This is code ported from 9.3 and must be reviewed as it may very well be useless
                certificate['privatekey'] = export_privatekey(certificate['privatekey'])

            if certificate.get('csr'):
                certificate['csr_path'] = os.path.join(
                    cert_path, '{0}.csr'.format(certificate['name']))

            return certificate

        return self.datastore.query('crypto.certificates', *(filter or []), callback=extend, **(params or {}))


@accepts(h.all_of(
    h.ref('crypto-certificate'),
    h.required('type', 'name', 'country', 'state', 'city', 'organization', 'email', 'common'),
))
@description('Creates a certificate')
class CertificateCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating certificate"

    def describe(self, certificate):
        return TaskDescription("Creating certificate {name}", name=certificate['name'])

    def verify(self, certificate):
        certificate['selfsigned'] = certificate.get('selfsigned', False)
        certificate['signing_ca_name'] = certificate.get('signing_ca_name', False)

        if '"' in certificate['name']:
            raise VerifyException(errno.EINVAL, 'Provide certificate name without : `"`')

        if self.datastore.exists('crypto.certificates', ('name', '=', certificate['name'])):
            raise VerifyException(errno.EEXIST,
                                  'Certificate named "{0}" already exists'.format(certificate['name']))

        if certificate['type'] in ('CERT_INTERNAL', 'CA_INTERNAL', 'CA_INTERMEDIATE'):
            if not certificate['selfsigned'] and not certificate['signing_ca_name']:
                raise VerifyException(errno.ENOENT,
                                      'Either "selfsigned" or "signing_ca_name" field value must be specified')
            if certificate['selfsigned'] and certificate['signing_ca_name']:
                raise VerifyException(errno.ENOENT,
                                      'Only one of "selfsigned","signing_ca_name" fields should be specified')

        if certificate['type'] == 'CA_INTERMEDIATE':
            if not certificate['signing_ca_name']:
                raise VerifyException(errno.ENOENT, '"signing_ca_name" field value not specified')

        if certificate['signing_ca_name']:
            if not self.datastore.exists('crypto.certificates', ('name', '=', certificate['signing_ca_name'])):
                raise VerifyException(errno.ENOENT,
                                      'Signing certificate "{0}" not found'.format(certificate['signing_ca_name']))

        return ['system']

    def run(self, certificate):
        def get_utc_string_from_asn1generalizedtime(asn1):
            return str(datetime.datetime.strptime(asn1, "%Y%m%d%H%M%SZ").replace(tzinfo=UTC))

        def get_x509_inst(cert_info):
            cert = crypto.X509()
            map_x509_subject_info(cert, cert_info)
            if not cert_info.get('serial'):
                cert.set_serial_number(get_next_x509_serial_number())
            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(cert_info['lifetime'] * (60 * 60 * 24))
            return cert

        def get_x509req_inst(cert_info):
            req = crypto.X509Req()
            map_x509_subject_info(req, cert_info)
            return req

        def get_next_x509_serial_number():
            return int(time.time())

        def map_x509_subject_info(x509, info):
            x509.get_subject().C = info['country']
            x509.get_subject().ST = info['state']
            x509.get_subject().L = info['city']
            x509.get_subject().O = info['organization']
            x509.get_subject().CN = info['common']
            x509.get_subject().emailAddress = info['email']

        def add_x509_extensions(x509, cert_type):
            if cert_type == 'CERT_INTERNAL':
                x509.add_extensions([
                    crypto.X509Extension("subjectKeyIdentifier".encode('utf-8'), False, "hash".encode('utf-8'),
                                         subject=x509)
                ])
            if cert_type == 'CA_INTERNAL':
                x509.add_extensions([
                    crypto.X509Extension("basicConstraints".encode('utf-8'), True, "CA:TRUE".encode('utf-8')),
                    crypto.X509Extension("keyUsage".encode('utf-8'), True, "keyCertSign, cRLSign".encode('utf-8')),
                    crypto.X509Extension("subjectKeyIdentifier".encode('utf-8'), False, "hash".encode('utf-8'),
                                         subject=x509),
                ])
            if cert_type == 'CA_INTERMEDIATE':
                x509.add_extensions([
                    crypto.X509Extension("basicConstraints".encode('utf-8'), True, "CA:TRUE".encode('utf-8')),
                    crypto.X509Extension("keyUsage".encode('utf-8'), True, "keyCertSign, cRLSign".encode('utf-8')),
                    crypto.X509Extension("subjectKeyIdentifier".encode('utf-8'), False, "hash".encode('utf-8'),
                                         subject=x509),
                ])

        def generate_key(key_length):
            k = crypto.PKey()
            k.generate_key(crypto.TYPE_RSA, key_length)
            return k

        try:
            certificate['selfsigned'] = certificate.get('selfsigned', False)
            certificate['key_length'] = certificate.get('key_length', 2048)
            certificate['digest_algorithm'] = certificate.get('digest_algorithm', 'SHA256')
            certificate['lifetime'] = certificate.get('lifetime', 3650)

            key = generate_key(certificate['key_length'])

            if certificate['type'] == 'CERT_CSR':
                x509 = get_x509req_inst(certificate)
                x509.set_pubkey(key)
                x509.sign(key, certificate['digest_algorithm'])

                certificate['csr'] = crypto.dump_certificate_request(crypto.FILETYPE_PEM, x509).decode('utf-8')
                certificate['privatekey'] = crypto.dump_privatekey(crypto.FILETYPE_PEM, key).decode('utf-8')
            else:
                x509 = get_x509_inst(certificate)
                x509.set_pubkey(key)
                add_x509_extensions(x509, certificate['type'])

                if certificate['selfsigned']:
                    signing_x509 = x509
                    signkey = key
                else:
                    signing_cert_db_entry = self.datastore.get_one('crypto.certificates',
                                                                   ('name', '=', certificate['signing_ca_name']))
                    certificate['signing_ca_id'] = signing_cert_db_entry['id']
                    signing_x509 = crypto.load_certificate(crypto.FILETYPE_PEM, signing_cert_db_entry['certificate'])
                    signkey = load_privatekey(signing_cert_db_entry['privatekey'])

                x509.set_issuer(signing_x509.get_subject())
                x509.sign(signkey, certificate['digest_algorithm'])

                certificate['not_before'] = get_utc_string_from_asn1generalizedtime(x509.get_notBefore().decode('utf-8'))
                certificate['not_after'] = get_utc_string_from_asn1generalizedtime(x509.get_notAfter().decode('utf-8'))
                certificate['serial'] = x509.get_serial_number()
                certificate['certificate'] = crypto.dump_certificate(crypto.FILETYPE_PEM, x509).decode('utf-8')
                certificate['privatekey'] = crypto.dump_privatekey(crypto.FILETYPE_PEM, key).decode('utf-8')

            pkey = self.datastore.insert('crypto.certificates', certificate)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'crypto')
            self.dispatcher.dispatch_event('crypto.certificate.changed', {
                'operation': 'create',
                'ids': [pkey]
            })
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot create certificate: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))

        return pkey


@accepts(h.all_of(
    h.ref('crypto-certificate'),
    h.required('name', 'type', 'certificate'),
))
@description('Imports a certificate')
class CertificateImportTask(Task):
    @classmethod
    def early_describe(cls):
        return "Importing certificate"

    def describe(self, certificate):
        return TaskDescription("Importing certificate {name}", name=certificate['name'])

    def verify(self, certificate):
        if self.datastore.exists('crypto.certificates', ('name', '=', certificate['name'])):
            raise VerifyException(errno.EEXIST, 'Certificate named "{0}" already exists'.format(certificate['name']))

        if certificate['type'] not in ('CERT_EXISTING', 'CA_EXISTING'):
            raise VerifyException(errno.EINVAL, 'Invalid certificate type')

        if certificate['type'] == 'CERT_EXISTING':
            if 'privatekey' not in certificate or 'passphrase' not in certificate:
                raise VerifyException(errno.EINVAL, 'privatekey and passphrase required to import certificate')

        try:
            if 'privatekey' in certificate:
                load_privatekey(certificate['privatekey'], certificate.get('passphrase'))
        except Exception:
            raise VerifyException(errno.EINVAL, 'Invalid passphrase')

        return ['system']

    def run(self, certificate):
        certificate.update(load_certificate(certificate['certificate']))

        if 'privatekey' in certificate:
            certificate['privatekey'] = export_privatekey(
                certificate['privatekey'], certificate['passphrase'])

        try:
            pkey = self.datastore.insert('crypto.certificates', certificate)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'crypto')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot import certificate: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('crypto.certificate.changed', {
            'operation': 'create',
            'ids': [pkey]
        })

        return pkey


@accepts(str, h.all_of(
    h.ref('crypto-certificate'),
))
@description('Updates a certificate')
class CertificateUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating certificate"

    def describe(self, id, updated_fields):
        cert = self.datastore.get_by_id('crypto.certificates', id)
        return TaskDescription("Updating certificate {name}", name=cert.get('name', '') if cert else '')

    def verify(self, id, updated_fields):
        certificate = self.datastore.get_by_id('crypto.certificates', id)
        if certificate is None:
            raise VerifyException(errno.ENOENT, 'Certificate ID {0} does not exist'.format(id))

        if 'name' in updated_fields and self.datastore.exists(
            'crypto.certificates', ('name', '=', updated_fields['name']), ('id', '!=', id)
        ):
            raise VerifyException(errno.EEXIST, 'Certificate with given name already exists')

        if not certificate['type'].startswith('CA_') or certificate['type'] != 'CERT_CSR':
            raise VerifyException(errno.EINVAL, 'Invalid certificate type: {0}'.format(certificate['type']))

        try:
            if 'certificate' in updated_fields:
                load_certificate(updated_fields['certificate'])
        except crypto.Error as e:
            raise VerifyException(errno.EINVAL, 'Invalid certificate: {0}'.format(str(e)))

        return ['system']

    def run(self, id, updated_fields):
        try:
            certificate = self.datastore.get_by_id('crypto.certificates', id)
            if certificate['type'] == 'CERT_CSR':
                certificate['certificate'] = updated_fields['certificate']
                certificate['type'] = 'CERT_EXISTING'
            else:
                if 'name' in updated_fields:
                    certificate['name'] = updated_fields['name']
                if 'certificate' in updated_fields:
                    certificate['certificate'] = updated_fields['certificate']
                if 'privatekey' in updated_fields:
                    certificate['privatekey'] = updated_fields['privatekey']
                if 'serial' in updated_fields:
                    certificate['serial'] = updated_fields['serial']

            pkey = self.datastore.update('crypto.certificates', id, certificate)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'crypto')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot update certificate: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('crypto.certificate.changed', {
            'operation': 'update',
            'ids': [id]
        })

        return pkey


@accepts(str)
@description('Deletes a certificate')
class CertificateDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting certificate"

    def describe(self, id):
        cert = self.datastore.get_by_id('crypto.certificates', id)
        return TaskDescription("Deleting certificate {name}", name=cert.get('name', '') if cert else '')

    def verify(self, id):
        if not self.datastore.exists('crypto.certificates', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Certificate ID {0} does not exist'.format(id))
        return ['system']

    def run(self, id):
        def get_subject_cert_id_and_name():
            return self.datastore.query('crypto.certificates', ('id', '=', id), select=('id', 'name'))

        def get_related_certs_ids_and_names(id):
            certs = self.datastore.query('crypto.certificates', ('signing_ca_id', '=', id), select=('id', 'name'))
            if not certs:
                return []
            nested = []
            for (cid, _) in certs:
                nested.extend(get_related_certs_ids_and_names(cid))
            return certs + nested

        certs = get_related_certs_ids_and_names(id)
        certs.extend(get_subject_cert_id_and_name())

        for (cid, cname) in certs:
            if not self.dispatcher.run_hook('crypto.pre_delete', cid):
                raise TaskException(errno.EBUSY, 'Certificate in use: {0}'.format(str(cname)))

        try:
            for (cid, _) in certs:
                self.datastore.delete('crypto.certificates', cid)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'crypto')
            self.dispatcher.dispatch_event('crypto.certificate.changed', {
                'operation': 'delete',
                'ids': [cid for (cid, _) in certs]
            })
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot delete certificate: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))


def _init(dispatcher, plugin):
    plugin.register_schema_definition('crypto-certificate', {
        'type': 'object',
        'properties': {
            'type': {'$ref': 'crypto-certificate-type'},
            'name': {'type': 'string'},
            'certificate': {'type': 'string'},
            'privatekey': {'type': 'string'},
            'csr': {'type': 'string'},
            'key_length': {'type': 'integer'},
            'digest_algorithm': {'$ref': 'crypto-certificate-digestalgorithm'},
            'lifetime': {'type': 'integer'},
            'not_before': {'type': 'string'},
            'not_after': {'type': 'string'},
            'country': {'type': 'string'},
            'state': {'type': 'string'},
            'city': {'type': 'string'},
            'organization': {'type': 'string'},
            'email': {'type': 'string'},
            'common': {'type': 'string'},
            'serial': {'type': 'integer'},
            'selfsigned': {'type': 'boolean'},
            'signing_ca_name': {'type': 'string'},
            'signing_ca_id': {'type': 'string'},
            'dn': {'type': 'string', 'readOnly': True},
            'valid_from': {'type': ['string', 'null'], 'readOnly': True},
            'valid_until': {'type': ['string', 'null'], 'readOnly': True},
            'certificate_path': {'type': ['string', 'null'], 'readOnly': True},
            'privatekey_path': {'type': ['string', 'null'], 'readOnly': True},
            'csr_path': {'type': ['string', 'null'], 'readOnly': True},
        },
        'additionalProperties': False,
    })

    plugin.register_schema_definition('crypto-certificate-type', {
        'type': 'string',
        'enum': ['CA_EXISTING', 'CA_INTERMEDIATE', 'CA_INTERNAL',
                 'CERT_CSR', 'CERT_EXISTING', 'CERT_INTERMEDIATE', 'CERT_INTERNAL']
    })

    plugin.register_schema_definition('crypto-certificate-digestalgorithm', {
        'type': 'string',
        'enum': ['SHA1', 'SHA224', 'SHA256', 'SHA384', 'SHA512']
    })

    plugin.register_provider('crypto.certificate', CertificateProvider)

    plugin.register_task_handler('crypto.certificate.create', CertificateCreateTask)
    plugin.register_task_handler('crypto.certificate.update', CertificateUpdateTask)
    plugin.register_task_handler('crypto.certificate.import', CertificateImportTask)
    plugin.register_task_handler('crypto.certificate.delete', CertificateDeleteTask)

    plugin.register_hook('crypto.pre_delete')

    # Register event types
    plugin.register_event_type('crypto.certificate.changed')
