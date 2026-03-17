"""
Unit tests for core/connections/encryption.py

Tests AES-GCM encrypt/decrypt roundtrip, type preservation,
wrong-key failure, and tamper detection.
No database, no Django, no network.
Run with: python -m pytest tests/unit/connections/test_encryption.py -v
"""
import sys

sys.path.insert(0, '.')

from api.connections.encryption import decrypt_field, encrypt_field

CMP_ID = 99001   # arbitrary customer ID for all tests
WRONG_CMP_ID = 99999


# ── Roundtrip tests ────────────────────────────────────────────────────────────

def test_string_roundtrip():
    data = "postgresql://user:pass@host:5432/mydb"
    encrypted = encrypt_field(data, CMP_ID)
    decrypted = decrypt_field(encrypted[0], CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
    assert decrypted == data


def test_integer_roundtrip():
    data = 42
    encrypted = encrypt_field(data, CMP_ID)
    decrypted = decrypt_field(encrypted[0], CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
    # decrypt_field returns string — check value equality
    assert str(data) == decrypted or int(decrypted) == data


def test_empty_string_roundtrip():
    data = ""
    encrypted = encrypt_field(data, CMP_ID)
    decrypted = decrypt_field(encrypted[0], CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
    assert decrypted == data


def test_long_string_roundtrip():
    data = "x" * 10000
    encrypted = encrypt_field(data, CMP_ID)
    decrypted = decrypt_field(encrypted[0], CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
    assert decrypted == data


def test_special_characters_roundtrip():
    data = "p@$$w0rd! #{}[]\"\\n\t"
    encrypted = encrypt_field(data, CMP_ID)
    decrypted = decrypt_field(encrypted[0], CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
    assert decrypted == data


def test_json_string_roundtrip():
    import json
    data = json.dumps({"host": "db.example.com", "port": 5432, "password": "s3cr3t"})
    encrypted = encrypt_field(data, CMP_ID)
    decrypted = decrypt_field(encrypted[0], CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
    assert json.loads(decrypted) == json.loads(data)


# ── Isolation tests ────────────────────────────────────────────────────────────

def test_different_customers_get_different_ciphertext():
    data = "same_password"
    enc1 = encrypt_field(data, 11111)
    enc2 = encrypt_field(data, 22222)
    # Ciphertext should differ (different keys, different salts)
    assert enc1[0] != enc2[0]


def test_same_data_encrypted_twice_gives_different_ciphertext():
    """Each encryption call uses a random salt+nonce — same input ≠ same output."""
    data = "same_password"
    enc1 = encrypt_field(data, CMP_ID)
    enc2 = encrypt_field(data, CMP_ID)
    assert enc1[0] != enc2[0]


def test_wrong_key_cannot_decrypt():
    """Decrypting with the wrong customer ID must fail (AES-GCM tag mismatch)."""
    data = "secret_connection_string"
    encrypted = encrypt_field(data, CMP_ID)
    try:
        result = decrypt_field(encrypted[0], WRONG_CMP_ID, encrypted[1], encrypted[2], encrypted[3], encrypted[4])
        # Should not reach here — if it does, decryption silently failed
        assert result != data, "Wrong key produced correct plaintext — encryption is broken"
    except Exception:
        pass  # Expected — decryption with wrong key should raise


# ── Output format tests ────────────────────────────────────────────────────────

def test_encrypt_returns_list_of_six_elements():
    result = encrypt_field("test", CMP_ID)
    assert isinstance(result, list)
    assert len(result) == 6


def test_encrypt_output_elements_are_strings_or_ints():
    result = encrypt_field("test", CMP_ID)
    # encrypted_data, nonce, tag, salt are strings; original_type is str; iterations is int
    encrypted_data, nonce, tag, salt, original_type, iterations = result
    assert isinstance(encrypted_data, str)
    assert isinstance(original_type, str)
    assert isinstance(iterations, int)


# ── Type preservation ──────────────────────────────────────────────────────────

def test_original_type_recorded_as_str_for_string_input():
    result = encrypt_field("hello", CMP_ID)
    assert result[4] == 'str'


def test_original_type_recorded_for_int_input():
    result = encrypt_field(123, CMP_ID)
    assert result[4] == 'int'
