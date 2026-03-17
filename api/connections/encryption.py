"""
Encryption Service
Canonical location for AES-GCM field encryption/decryption.
Consolidated from: api/services/encryption_service.py, encryption/encryption.py
Both were identical — one file kept.
"""
"""
Encryption service for field encryption/decryption using AES-GCM mode.
"""
import base64
import json
import logging
import os

from cryptography.exceptions import InvalidKey, InvalidTag
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

def derive_key(cmp_id: int, salt: bytes, iterations: int = 100000) -> bytes:
    """
    Derives a key from the cmp_id and salt using PBKDF2-HMAC with SHA256.
    """
    try:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        key = kdf.derive(str(cmp_id).encode())
        logger.debug("[Key Derivation] CMP ID: {cmp_id}, Salt: {salt.hex()}, Key: {key.hex()}")
        return key
    except Exception as e:
        raise ValueError(f"Key derivation failed: {e}")

def encrypt_field(data, cmp_id: int, iterations: int = 100000):
    """
    Encrypt a field using AES-GCM mode.
    """
    original_type = type(data).__name__
    salt = os.urandom(16)  # Generate a new random salt for each encryption
    nonce = os.urandom(12)  # 12-byte nonce for AES-GCM

    # Derive the encryption key
    key = derive_key(cmp_id, salt, iterations)

    # Apply PKCS7 padding
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(str(data).encode()) + padder.finalize()

    # AES-GCM encryption
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

    # Output the encrypted data and metadata
    result = [
         base64.b64encode(encrypted_data).decode(),
         base64.b64encode(nonce).decode(),
         base64.b64encode(encryptor.tag).decode(),
         base64.b64encode(salt).decode(),
         original_type,
         iterations
    ]

    return result

def decrypt_field(encrypted_data: str, cmp_id: int, nonce: str, tag: str, salt: str, original_type: str, iterations: int = 100000):
    """
    Decrypt a field using AES-GCM mode.
    """
    try:

        # Decode inputs from base64
        encrypted_data = base64.b64decode(encrypted_data)
        nonce = base64.b64decode(nonce)
        tag = base64.b64decode(tag)
        salt = base64.b64decode(salt)

        # Derive the decryption key
        key = derive_key(cmp_id, salt, iterations)

        # AES-GCM decryption
        cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(encrypted_data) + decryptor.finalize()

        # Remove PKCS7 padding
        unpadder = padding.PKCS7(128).unpadder()
        data = unpadder.update(padded_data) + unpadder.finalize()

        if original_type == 'str':
            return data.decode('utf-8')
        elif original_type == 'int':
            return int(data.decode('utf-8'))
        elif original_type == 'float':
            return float(data.decode('utf-8'))
        elif original_type == 'dict':
            try:

                print("dict data",type(data))
                data_str = data.decode('utf-8')
                data_str = data_str.replace("'", '"')
                return json.loads(data_str)

            except json.JSONDecodeError as e:
                print(f"Error decoding dictionary: {e}")
                return None
        else:

            print("data",data)
            print("type o",type(data))
            return data  # Raw bytes if type is unknown

    except InvalidTag:
        raise ValueError("Decryption failed: Authentication tag mismatch or tampered data.")
    except InvalidKey:
        raise ValueError("Decryption failed: Invalid key.")
    except Exception as e:
        raise ValueError(f"Decryption failed: {e}")
