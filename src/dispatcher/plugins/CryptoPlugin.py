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
from datastore import DatastoreException
from freenas.dispatcher.rpc import RpcException, description, accepts
from freenas.dispatcher.rpc import SchemaHelper as h
from task import Provider, Task, TaskException, ValidationException, VerifyException, query, TaskDescription

from OpenSSL import crypto


def create_certificate(cert_info):
    cert = crypto.X509()
    cert.get_subject().C = cert_info['country']
    cert.get_subject().ST = cert_info['state']
    cert.get_subject().L = cert_info['city']
    cert.get_subject().O = cert_info['organization']
    cert.get_subject().CN = cert_info['common']
    cert.get_subject().emailAddress = cert_info['email']

    serial = cert_info.get('serial')
    if serial is not None:
        cert.set_serial_number(serial)

    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(cert_info['lifetime'] * (60 * 60 * 24))

    cert.set_issuer(cert.get_subject())
    return cert


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
    )


def generate_key(key_length):
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, key_length)
    return k


def load_certificate(buf):
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, buf)

    cert_info = {}
    cert_info['country'] = cert.get_subject().C
    cert_info['state'] = cert.get_subject().ST
    cert_info['city'] = cert.get_subject().L
    cert_info['organization'] = cert.get_subject().O
    cert_info['common'] = cert.get_subject().CN
    cert_info['email'] = cert.get_subject().emailAddress

    signature_algorithm = cert.get_signature_algorithm()
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
                certificate['certificate'] = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)

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
    def describe(self, certificate):
        return TaskDescription("Creating certificate {name}", name=certificate['name'])

    def verify(self, certificate):

        errors = ValidationException()

        if certificate['type'] == 'CERT_INTERNAL':
            if self.datastore.exists('crypto.certificates', ('name', '=', certificate['name'])):
                errors.add((0, 'name'), 'Certificate with given name already exists', code=errno.EEXIST)

            if not self.datastore.exists('crypto.certificates', ('id', '=', certificate['signedby'])):
                errors.add((0, 'signedby'), 'Signing certificate does not exist', code=errno.EEXIST)

            if '"' in certificate['name']:
                errors.add((
                    (0, 'name'),
                    'You cannot issue a certificate with a `"` in its name')
                )

        if certificate['type'] in ('CERT_INTERNAL', 'CA_INTERMEDIATE'):
            if 'signedby' not in certificate or not self.datastore.exists('crypto.certificates', ('id', '=', certificate['signedby'])):
                errors.add((0, 'signedby'), 'Signing Certificate does not exist', code=errno.EEXIST)

        if errors:
            raise errors

        return ['system']

    def run(self, certificate):

        try:
            certificate['key_length'] = certificate.get('key_length', 2048)
            certificate['digest_algorithm'] = certificate.get('digest_algorithm', 'SHA256')
            certificate['lifetime'] = certificate.get('lifetime', 3650)

            key = generate_key(certificate['key_length'])

            if certificate['type'] == 'CERT_INTERNAL':

                signing_cert = self.datastore.get_by_id('crypto.certificates', certificate['signedby'])

                signkey = load_privatekey(signing_cert['privatekey'])

                cert = create_certificate(certificate)
                cert.set_pubkey(key)
                cacert = crypto.load_certificate(crypto.FILETYPE_PEM, signing_cert['certificate'])

                cert.set_issuer(cacert.get_subject())

                cert.add_extensions([
                    crypto.X509Extension("subjectKeyIdentifier", False, "hash", subject=cert),
                ])
                cert.set_serial_number(signing_cert['serial'])
                cert.sign(signkey, str(certificate['digest_algorithm']))

                certificate['type'] = 'CERT_INTERNAL'
                certificate['certificate'] = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
                certificate['privatekey'] = crypto.dump_privatekey(crypto.FILETYPE_PEM, key)

                pkey = self.datastore.insert('crypto.certificates', certificate)

                signing_cert['serial'] += 1
                self.datastore.update('crypto.certificates', signing_cert['id'], signing_cert)

            if certificate['type'] == 'CERT_CSR':

                req = crypto.X509Req()
                req.get_subject().C = certificate['country']
                req.get_subject().ST = certificate['state']
                req.get_subject().L = certificate['city']
                req.get_subject().O = certificate['organization']
                req.get_subject().CN = certificate['common']
                req.get_subject().emailAddress = certificate['email']

                req.set_pubkey(key)
                req.sign(key, str(certificate['digest_algorithm']))

                certificate['csr'] = crypto.dump_certificate_request(crypto.FILETYPE_PEM, req)
                certificate['privatekey'] = crypto.dump_privatekey(crypto.FILETYPE_PEM, key)

                pkey = self.datastore.insert('crypto.certificates', certificate)

            if certificate['type'] == 'CA_INTERNAL':

                cert = create_certificate(certificate)
                cert.set_pubkey(key)
                cert.add_extensions([
                    crypto.X509Extension("basicConstraints", True, "CA:TRUE, pathlen:0"),
                    crypto.X509Extension("keyUsage", True, "keyCertSign, cRLSign"),
                    crypto.X509Extension("subjectKeyIdentifier", False, "hash", subject=cert),
                ])
                cert.set_serial_number(1)
                cert.sign(key, str(certificate['digest_algorithm']))

                certificate['certificate'] = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
                certificate['privatekey'] = crypto.dump_privatekey(crypto.FILETYPE_PEM, key)
                certificate['serial'] = 1

                pkey = self.datastore.insert('crypto.certificates', certificate)

            if certificate['type'] == 'CA_INTERMEDIATE':

                signing_cert = self.datastore.get_by_id('crypto.certificates', certificate['signedby'])

                signkey = load_privatekey(signing_cert['privatekey'])

                cert = create_certificate(certificate)
                cert.set_pubkey(key)
                cert.add_extensions([
                    crypto.X509Extension("basicConstraints", True, "CA:TRUE, pathlen:0"),
                    crypto.X509Extension("keyUsage", True, "keyCertSign, cRLSign"),
                    crypto.X509Extension("subjectKeyIdentifier", False, "hash", subject=cert),
                ])
                cert.set_serial_number(signing_cert['serial'])
                cert.sign(signkey, str(certificate['digest_algorithm']))

                certificate['certificate'] = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
                certificate['privatekey'] = crypto.dump_privatekey(crypto.FILETYPE_PEM, key)

                pkey = self.datastore.insert('crypto.certificates', certificate)

                signing_cert['serial'] += 1
                self.datastore.update('crypto.certificates', signing_cert['id'], signing_cert)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'crypto')

        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot create certificate: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('crypto.certificate.changed', {
            'operation': 'create',
            'ids': [pkey]
        })

        return pkey


@accepts(h.all_of(
    h.ref('crypto-certificate'),
    h.required('name', 'type', 'certificate'),
))
@description('Imports a certificate')
class CertificateImportTask(Task):
    def describe(self, certificate):
        return TaskDescription("Importing certificate {name}", name=certificate['name'])

    def verify(self, certificate):

        if self.datastore.exists('crypto.certificates', ('name', '=', certificate['name'])):
            raise VerifyException(errno.EEXIST, 'Certificate with given name already exists')

        if certificate['type'] not in ('CERT_EXISTING', 'CA_EXISTING'):
            raise VerifyException(errno.EINVAL, 'Invalid certificate type')

        errors = ValidationException()
        for i in ('country', 'state', 'city', 'organization', 'email', 'common'):
            if i in certificate:
                errors.add((0, i), '{0} is not valid in certificate import'.format(i))
        if errors:
            raise errors

        if certificate['type'] == 'CERT_EXISTING' and (
            'privatekey' not in certificate or
            'passphrase' not in certificate
        ):
            raise VerifyException(
                errno.EINVAL, 'privatekey and passphrase required to import certificate'
            )

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
    def describe(self, id, updated_fields):
        return TaskDescription("Updating certificate {name}", name=id)

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
    def describe(self, id):
        return TaskDescription("Deleting certificate {name}", name=id)

    def verify(self, id):
        certificate = self.datastore.get_by_id('crypto.certificates', id)
        if certificate is None:
            raise VerifyException(errno.ENOENT, 'Certificate ID {0} does not exist'.format(id))

        return ['system']

    def run(self, id):
        try:
            for i in self.datastore.query('crypto.certificates', ('signedby', '=', id)):
                self.datastore.delete('crypto.certificates', i['id'])

            self.datastore.delete('crypto.certificates', id)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'crypto')
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot delete certificate: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('crypto.certificate.changed', {
            'operation': 'delete',
            'ids': [id]
        })


def _init(dispatcher, plugin):
    plugin.register_schema_definition('crypto-certificate', {
        'type': 'object',
        'properties': {
            'type': {'type': 'string', 'enum': [
                'CA_EXISTING',
                'CA_INTERMEDIATE',
                'CA_INTERNAL',
                'CERT_CSR',
                'CERT_EXISTING',
                'CERT_INTERMEDIATE',
                'CERT_INTERNAL',
            ]},
            'name': {'type': 'string'},
            'certificate': {'type': 'string'},
            'privatekey': {'type': 'string'},
            'csr': {'type': 'string'},
            'key_length': {'type': 'integer'},
            'digest_algorithm': {'type': 'string', 'enum': [
                'SHA1',
                'SHA224',
                'SHA256',
                'SHA384',
                'SHA512',
            ]},
            'lifetime': {'type': 'integer'},
            'country': {'type': 'string'},
            'state': {'type': 'string'},
            'city': {'type': 'string'},
            'organization': {'type': 'string'},
            'email': {'type': 'string'},
            'common': {'type': 'string'},
            'serial': {'type': 'integer'},
            'signedby': {'type': 'string'},
            'dn': {'type': 'string', 'readOnly': True},
            'valid_from': {'type': ['string', 'null'], 'readOnly': True},
            'valid_until': {'type': ['string', 'null'], 'readOnly': True},
            'certificate_path': {'type': ['string', 'null'], 'readOnly': True},
            'privatekey_path': {'type': ['string', 'null'], 'readOnly': True},
            'csr_path': {'type': ['string', 'null'], 'readOnly': True},
        },
        'additionalProperties': False,
    })

    plugin.register_provider('crypto.certificate', CertificateProvider)

    plugin.register_task_handler('crypto.certificate.create', CertificateCreateTask)
    plugin.register_task_handler('crypto.certificate.update', CertificateUpdateTask)
    plugin.register_task_handler('crypto.certificate.import', CertificateImportTask)
    plugin.register_task_handler('crypto.certificate.delete', CertificateDeleteTask)

    # Register event types
    plugin.register_event_type('crypto.certificate.changed')
