"""
IEncryptionService port.
Abstracts field-level AES encryption for source/destination credentials.
"""
from abc import ABC, abstractmethod


class IEncryptionService(ABC):

    @abstractmethod
    def encrypt(self, plaintext: str, customer_id: str, created_on) -> str:
        """Encrypt plaintext. Returns encrypted blob as a JSON string."""

    @abstractmethod
    def decrypt(self, encrypted_blob: str, customer_id: str, created_on) -> str:
        """Decrypt blob. Returns original plaintext."""
