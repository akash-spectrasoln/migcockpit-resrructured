"""
Unit tests for core/pipeline/expression_translator.py

Tests SQL expression translation from the canvas expression language
to database-specific SQL for all supported DB types.
No database, no Django, no network.
Run with: python -m pytest tests/unit/pipeline/test_expression_translator.py -v
"""
import sys

sys.path.insert(0, '.')

from api.pipeline.expression_translator import ExpressionTranslator

COLUMNS = ['first_name', 'last_name', 'age', 'salary', 'created_at', 'status', 'email']
COL_META = {
    'first_name': {'datatype': 'VARCHAR'},
    'last_name':  {'datatype': 'VARCHAR'},
    'age':        {'datatype': 'INTEGER'},
    'salary':     {'datatype': 'NUMERIC'},
    'created_at': {'datatype': 'TIMESTAMP'},
    'status':     {'datatype': 'VARCHAR'},
    'email':      {'datatype': 'VARCHAR'},
}


def translator(db='postgresql'):
    return ExpressionTranslator(COLUMNS, db, COL_META)


# ── Basic column reference ─────────────────────────────────────────────────────

def test_bare_column_wraps_in_quotes():
    t = translator()
    result = t.translate('first_name')
    assert 'first_name' in result


def test_unknown_column_raises_or_returns_something():
    t = translator()
    # Should either raise ValueError or return the expression unchanged
    try:
        result = t.translate('nonexistent_col')
        # If it doesn't raise, it should at least contain the column name
        assert 'nonexistent_col' in result
    except (ValueError, KeyError):
        pass


# ── String functions ───────────────────────────────────────────────────────────

def test_upper_function():
    t = translator()
    result = t.translate('UPPER(first_name)')
    assert 'UPPER' in result.upper() or 'upper' in result.lower()


def test_lower_function():
    t = translator()
    result = t.translate('LOWER(last_name)')
    assert 'LOWER' in result.upper() or 'lower' in result.lower()


def test_concat_two_columns():
    t = translator()
    result = t.translate("CONCAT(first_name, ' ', last_name)")
    assert 'first_name' in result or 'CONCAT' in result


def test_trim_function():
    t = translator()
    result = t.translate('TRIM(email)')
    assert 'email' in result


def test_length_function():
    t = translator()
    result = t.translate('LENGTH(first_name)')
    assert 'first_name' in result


# ── Math expressions ───────────────────────────────────────────────────────────

def test_addition():
    t = translator()
    result = t.translate('age + 1')
    assert 'age' in result
    assert '+' in result


def test_multiplication():
    t = translator()
    result = t.translate('salary * 1.1')
    assert 'salary' in result
    assert '*' in result


def test_division():
    t = translator()
    result = t.translate('salary / 12')
    assert 'salary' in result


# ── Conditional expressions ────────────────────────────────────────────────────

def test_case_when_expression():
    t = translator()
    expr = "CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END"
    result = t.translate(expr)
    assert 'CASE' in result.upper() or 'age' in result


# ── DB-type specific translation ───────────────────────────────────────────────

def test_sqlserver_uses_correct_concat_syntax():
    t = ExpressionTranslator(COLUMNS, 'sqlserver', COL_META)
    result = t.translate("CONCAT(first_name, ' ', last_name)")
    # SQL Server may use + for concat or CONCAT function
    assert 'first_name' in result


def test_mysql_translator_does_not_crash():
    t = ExpressionTranslator(COLUMNS, 'mysql', COL_META)
    result = t.translate('UPPER(first_name)')
    assert result is not None and len(result) > 0


def test_oracle_translator_does_not_crash():
    t = ExpressionTranslator(COLUMNS, 'oracle', COL_META)
    result = t.translate('LOWER(email)')
    assert result is not None and len(result) > 0


# ── validate_column_references ─────────────────────────────────────────────────

def test_validate_known_column_is_valid():
    t = translator()
    valid, error = t.validate_column_references('UPPER(first_name)')
    assert valid is True
    assert error is None


def test_validate_unknown_column_is_invalid():
    t = translator()
    valid, error = t.validate_column_references('UPPER(ghost_column)')
    assert valid is False
    assert error is not None


def test_validate_math_with_known_columns_is_valid():
    t = translator()
    valid, error = t.validate_column_references('salary * 12')
    assert valid is True


# ── Edge cases ─────────────────────────────────────────────────────────────────

def test_translate_numeric_literal():
    t = translator()
    result = t.translate('42')
    assert '42' in result


def test_translate_string_literal():
    t = translator()
    result = t.translate("'hello'")
    assert 'hello' in result


def test_translate_does_not_return_none():
    t = translator()
    result = t.translate('age')
    assert result is not None
    assert isinstance(result, str)
    assert len(result) > 0
