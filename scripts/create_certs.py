"""Create a project-only CA and mTLS identities. Never print private key material."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import argparse
import os

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID


def create(directory):
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise RuntimeError('Refusing to overwrite an existing certificate directory')
    os.chmod(directory, 0o700)
    now = datetime.now(timezone.utc)
    def key():
        return rsa.generate_private_key(public_exponent=65537, key_size=3072)
    def name(cn):
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    ca_key = key()
    ca = (x509.CertificateBuilder().subject_name(name('stream-chat-ca')).issuer_name(name('stream-chat-ca'))
        .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now-timedelta(minutes=5)).not_valid_after(now+timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True).sign(ca_key, hashes.SHA256()))
    def save(label, private, cert):
        for suffix, data in [('key', private.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption())),
            ('crt', cert.public_bytes(serialization.Encoding.PEM))]:
            path = directory / f'{label}.{suffix}'
            with path.open('xb') as out:
                os.chmod(path, 0o600)
                out.write(data)
    save('ca', ca_key, ca)
    for label, cn, usage in [('server', 'model-worker', ExtendedKeyUsageOID.SERVER_AUTH),
                              ('client', 'chat-service', ExtendedKeyUsageOID.CLIENT_AUTH)]:
        private = key()
        cert = (x509.CertificateBuilder().subject_name(name(cn)).issuer_name(ca.subject)
            .public_key(private.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-timedelta(minutes=5)).not_valid_after(now+timedelta(days=90))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(cn)]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=True).sign(ca_key, hashes.SHA256()))
        save(label, private, cert)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    create(parser.parse_args().directory)
    print('Created CA and service certificates. Leaf certificates expire in 90 days.')
