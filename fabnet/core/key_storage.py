#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.core.key_storage
@author Konstantin Andrusenko
@date October 28, 2012

This module contains the implementation of
a basic secure saved key storage.
"""
import os
import tempfile
import zipfile
from datetime import datetime

from M2Crypto import X509
from M2Crypto.SSL import Context

from constants import NODE_CERTIFICATE, CLIENT_CERTIFICATE, \
                        NODE_ROLE, CLIENT_ROLE

NB_CERT_FILENAME = 'nodes.idepositbox.com.pem'
CB_CERT_FILENAME = 'clients.idepositbox.com.pem'
NODE_CERT_FILENAME = 'node_certificate.pem'
NODE_PRIKEY_FILENAME = 'node_prikey'

class NeedCertificate(Exception):
    pass

class ExpiredCertificate(Exception):
    pass


class AbstractKeyStorage:
    def __init__(self, ks_path, passwd):
        self._nodes_base_pubkey = None
        self._client_base_pubkey = None
        self._node_cert = None
        self._node_prikey = None

        self._load_key_storage(ks_path, passwd)

    def _load_key_storage(self, ks_path, passwd):
        pass

    def get_node_cert(self):
        return self._node_cert

    def get_node_cert_key(self):
        cert = X509.load_cert_string(self._node_cert)
        return cert.get_fingerprint()
        #return cert.get_ext('authorityKeyIdentifier').get_value()[5:].strip().replace(':','')

    def verify_cert(self, cert_str):
        '''Verify certificate and return certificate role'''
        cert = X509.load_cert_string(str(cert_str))

        cert_end_dt = cert.get_not_after().get_datetime().utctimetuple()
        if cert_end_dt < datetime.utcnow().utctimetuple():
            raise Exception('Certificate is out of date')

        cert_type = cert.get_subject().CN
        role = None
        if cert_type == NODE_CERTIFICATE:
            root_pubkey = self._nodes_base_pubkey
            role = NODE_ROLE
        elif cert_type == CLIENT_CERTIFICATE:
            root_pubkey = self._client_base_pubkey
            role = CLIENT_ROLE
        else:
            raise Exception('Unknown certificate type: %s'%cert_type)

        if not cert.verify(root_pubkey):
            raise Exception('Certification')

        return role

    def get_node_context(self):
        _, certfile = tempfile.mkstemp()
        _, keyfile = tempfile.mkstemp()
        try:
            open(certfile, 'w').write(self._node_cert)
            open(keyfile, 'w').write(self._node_prikey)
            context = Context()
            context.load_cert(certfile, keyfile)

            return context
        finally:
            os.unlink(certfile)
            os.unlink(keyfile)


class FileBasedKeyStorage(AbstractKeyStorage):
    def _load_key_storage(self, ks_path, passwd):
        if not os.path.exists(ks_path):
            raise Exception('Key storage file %s does not found!'%ks_path)

        storage = zipfile.ZipFile(ks_path)
        storage.setpassword(passwd)

        def read_file(f_name):
            f_obj = storage.open(f_name)
            data = f_obj.read()
            f_obj.close()
            return data

        nb_cert = X509.load_cert_string(read_file(NB_CERT_FILENAME))
        self._nodes_base_pubkey = nb_cert.get_pubkey()

        cb_cert = X509.load_cert_string(read_file(CB_CERT_FILENAME))
        self._client_base_pubkey = cb_cert.get_pubkey()

        self._node_cert = read_file(NODE_CERT_FILENAME)
        self._node_prikey = read_file(NODE_PRIKEY_FILENAME)

        storage.close()
