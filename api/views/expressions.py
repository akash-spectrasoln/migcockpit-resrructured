"""
Expression API Views
Merged from: expression.py, expression_testing.py, expression_validation.py
"""

# ============================================================
# From: expression.py
# ============================================================
"""
Expression and column-related API views.
Handles column filtering, statistics, and sequence management.
"""
import json
import logging

from django.conf import settings
import psycopg2
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication

try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import pycountry
except ImportError:
    pycountry = None
try:
    import pgeocode
except ImportError:
    pgeocode = None
try:
    import country_converter as cc
except ImportError:
    cc = None
try:
    import pint
except ImportError:
    pint = None

logger = logging.getLogger(__name__)

class FilterColumnValuesView(APIView):
    """
    API endpoint for filtering column values.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

# Helper functions for column statistics
def get_column_info(column_name):
    """
    Get column information from column_categories.json
    """
    with open('column_categories.json') as f:
        column_categories = json.load(f)
        for category, value in column_categories.items():
            if isinstance(value,list) and column_name in value:
                return category
            elif isinstance(value,dict) and "columns" in value and column_name in value["columns"]:
                return category
    return None

 # Helper function for column statistics
def get_category_columns(column_name):
    """
    Get category columns from column_categories.json
    """
    with open('column_categories.json') as f:
        column_categories = json.load(f)
        return column_categories.get(column_name, None)

# hepler function for column statistics
def check_columns_in_table(cursor, schema, table_name, category_columns):
    """
    Check if any columns from category_columns exist in the table
    """
    if not category_columns:
        return []

    # Create placeholders for the IN clause
    ','.join(['%s'] * len(category_columns))

    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        AND column_name IN ({placeholders})
    """

    cursor.execute(query, [schema, table_name, *category_columns])
    existing_columns = [row[0] for row in cursor.fetchall()]

    if len(existing_columns) > 0:
        return existing_columns[0]
    return None

class ColumnStatisticsView(APIView):
    """
    API endpoint for getting column statistics.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        column_name = request.data.get('column_name')
        table_name = request.data.get('table_name')
        schema = request.data.get('schema')
        # user_email = request.data.get('user')

        if not column_name or not table_name or not schema:
            return Response(
                {"error": "Column name, table name and schema are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        column_category = get_column_info(column_name)

        if not column_category:
            return Response(
                {"error": "Column category not found."},
                status=status.HTTP_400_BAD_REQUEST
            )
        user = request.user
        # user=User.objects.get(email=user_email)
        customer = user.cust_id
        if not customer:
            return Response(
                {"error": "User is not associated with any customer."},
                status=status.HTTP_400_BAD_REQUEST
            )

        if pd is None:
            return Response(
                {
                    "error": "pandas is not available. Column statistics for date/country/units require pandas. "
                    "Fix with: pip uninstall -y pandas && pip install \"pandas==2.2.3\""
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE
            )
        if column_category in ("country_code", "currency_code", "subdivision_code") and pycountry is None:
            return Response(
                {"error": "pycountry is not available. Install with: pip install pycountry"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if column_category == "postal_code" and (pgeocode is None or pycountry is None):
            return Response(
                {"error": "pgeocode/pycountry not available (pandas may be broken). Fix with: pip uninstall -y pandas && pip install \"pandas==2.2.3\""},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if column_category == "subdivision_code" and cc is None:
            return Response(
                {"error": "country_converter is not available. Install with: pip install country_converter"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if column_category == "units" and pint is None:
            return Response(
                {"error": "pint is not available. Install with: pip install pint"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        try:
            connection = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=customer.cust_db
            )
            with connection.cursor() as cursor:

                if column_category == 'numerical':
                    cursor.execute("""
                        SELECT
                            MIN("{column_name}"),
                            MAX("{column_name}"),
                            COUNT(*),
                            COUNT(CASE WHEN "{column_name}" = 0 THEN 1 END) as zero_count,
                            COUNT(CASE WHEN "{column_name}" IS NULL THEN 1 END) as null_count
                        FROM "{schema}"."{table_name}"
                    """)

                    result = cursor.fetchone()
                    min_val, max_val, total_count, zero_count, null_count = result

                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "statistics": {
                            "min_value": min_val,
                            "max_value": max_val,
                            "total_count": total_count,
                            "zero_count": zero_count,
                            "null_count": null_count
                        }
                    })

                elif column_category == 'string':
                    # Get basic statistics
                    cursor.execute("""
                        SELECT
                            COUNT(*) as total_count,
                            COUNT(CASE WHEN "{column_name}" IS NULL THEN 1 END) as null_count
                        FROM "{schema}"."{table_name}"
                    """)

                    result = cursor.fetchone()
                    total_count, null_count = result

                    # Get distinct values and their counts
                    cursor.execute("""
                        SELECT
                            "{column_name}" as value,
                            COUNT(*) as count
                        FROM "{schema}"."{table_name}"
                        WHERE "{column_name}" IS NOT NULL
                        GROUP BY "{column_name}"
                        ORDER BY "{column_name}" ASC
                    """)

                    distinct_values = cursor.fetchall()
                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "statistics": {
                            "total_count": total_count,
                            "null_count": null_count,
                            "distinct_values": [
                                {"value": row[0], "count": row[1]}
                                for row in distinct_values
                            ]
                        }
                    })

                elif column_category == 'date':
                    # Load data using pandas for better date analysis
                    cursor.execute("""
                        SELECT "{column_name}"
                        FROM "{schema}"."{table_name}"
                    """)

                    # Fetch all data and create DataFrame
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=[column_name])

                    # Basic statistics
                    total_count = len(df)
                    null_count = df[column_name].isnull().sum()
                    non_null_count = df[column_name].notna().sum()

                    # Count invalid dates and get min/max dates using pandas
                    non_null_data = df[column_name].dropna()
                    invalid_dates = []
                    invalid_date_count = 0
                    min_date = None
                    max_date = None

                    if len(non_null_data) > 0:
                        # Convert to datetime with errors='coerce' - invalid dates become NaT
                        converted_dates = pd.to_datetime(non_null_data, errors='coerce')

                        # Find invalid dates (where conversion resulted in NaT)
                        invalid_mask = converted_dates.isnull()
                        invalid_date_count = invalid_mask.sum()

                        # Get valid dates (non-NaT)
                        valid_dates = converted_dates.dropna()

                        if len(valid_dates) > 0:
                            min_date = str(valid_dates.min())
                            max_date = str(valid_dates.max())

                    # Get the actual invalid date values for display
                    if invalid_date_count > 0:
                        invalid_dates = non_null_data[invalid_mask].tolist()
                        # Convert to string and remove duplicates
                        invalid_dates = list({str(date) for date in invalid_dates})

                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "statistics": {
                            "total_count": int(total_count),
                            "null_count": int(null_count),
                            "non_null_count": int(non_null_count),
                            "min_date": min_date,
                            "max_date": max_date,
                            "invalid_date_count": int(invalid_date_count),
                            "invalid_dates": invalid_dates
                        }
                    })

                elif column_category == 'country_code':
                    # Load data using pandas for better country code analysis
                    cursor.execute("""
                        SELECT "{column_name}"
                        FROM "{schema}"."{table_name}"
                    """)

                    # Fetch all data and create DataFrame
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=[column_name])
                    # Check if maximum character length in this column is more than three
                    max_char_len = df[column_name].dropna().astype(str).apply(len).max() if not df.empty else 0

                    if max_char_len > 3:
                        return Response(
                            {"error": "Maximum character length in this column is less than three."},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # Basic statistics - null count and invalid country codes
                    null_count = df[column_name].isnull().sum()

                    # Get distinct country codes with counts
                    distinct_countries = []
                    # Get distinct country codes and their counts
                    country_counts = df[column_name].dropna().astype(str).str.strip().str.upper().value_counts()
                    distinct_countries = [
                        {"value": code, "count": int(count)}
                        for code, count in country_counts.items()
                    ]

                    # Validate country codes (ISO 3166-1 Alpha-3)
                    non_null_data = df[column_name].dropna()
                    invalid_country_codes = []
                    invalid_country_code_count = 0

                    if len(non_null_data) > 0:
                        # Get all valid ISO 3166-1 Alpha-3 country codes
                        valid_country_codes = {country.alpha_2 for country in pycountry.countries} | {country.alpha_3 for country in pycountry.countries}

                        # Convert to string and strip whitespace, then to uppercase
                        non_null_data_str = non_null_data.astype(str).str.strip().str.upper()

                        # Check length and validity
                        length_invalid_mask = non_null_data_str.str.len() > 3
                        not_in_valid_mask = ~non_null_data_str.isin(valid_country_codes)

                        # Combine masks using OR operator to find invalid entries
                        invalid_mask = length_invalid_mask | not_in_valid_mask

                        # Extract unique invalid country codes
                        invalid_country_codes = non_null_data_str[invalid_mask].unique().tolist()
                        invalid_country_code_count = len(invalid_country_codes)

                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "statistics": {
                            "null_count": int(null_count),
                            "invalid_country_code_count": int(invalid_country_code_count),
                            "invalid_country_codes": invalid_country_codes,
                            "distinct_countries": distinct_countries
                        }
                        # "mismatched_country_codes": mismatched_country_codes
                    })

                elif column_category == 'currency_code':
                    # Load data using pandas for better currency code analysis
                    cursor.execute("""
                        SELECT "{column_name}"
                        FROM "{schema}"."{table_name}"
                    """)

                    # Fetch all data and create DataFrame
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=[column_name])

                    # Basic statistics - null count
                    null_count = df[column_name].isnull().sum()

                    # Get distinct currency codes and their counts
                    currency_counts = df[column_name].dropna().astype(str).str.strip().value_counts()
                    distinct_currencies = [
                        {"value": code, "count": int(count)}
                        for code, count in currency_counts.items()
                    ]

                    # Validate currency codes
                    non_null_data = df[column_name].dropna()
                    invalid_currency_codes = []
                    invalid_currency_code_count = 0

                    if len(non_null_data) > 0:
                        # Get all valid currency codes from pycountry
                        valid_currencies = {currency.alpha_3 for currency in pycountry.currencies}

                        # Convert to string and strip whitespace, then to uppercase
                        non_null_data_str = non_null_data.astype(str).str.strip().str.upper()

                        # Check validity of currency codes
                        not_in_valid_mask = ~non_null_data_str.isin(valid_currencies)

                        # Extract unique invalid currency codes (original values as fetched from DB)
                        invalid_currency_codes = non_null_data[not_in_valid_mask].unique().tolist()
                        invalid_currency_code_count = len(invalid_currency_codes)

                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "statistics": {
                            "null_count": int(null_count),
                            "invalid_currency_code_count": int(invalid_currency_code_count),
                            "invalid_currency_codes": invalid_currency_codes,
                            "distinct_currencies": distinct_currencies
                        }
                    })
                elif column_category == 'region_category':
                    # First check if country_column_in_table exists
                    country_columns = get_category_columns('country_code')
                    country_column_in_table = None
                    if country_columns:
                        country_column_in_table = check_columns_in_table(cursor, schema, table_name, country_columns)

                    # Only proceed with validation if country_column_in_table exists
                    if country_column_in_table:
                        # Load data using pandas for better region code analysis
                        cursor.execute("""
                            SELECT "{country_column_in_table}", "{column_name}"
                            FROM "{schema}"."{table_name}"
                        """)
                        # Fetch all data and create DataFrame
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=[country_column_in_table, column_name])

                        # Basic statistics - null count
                        null_count = df[column_name].isnull().sum()

                        # Validate region codes
                        non_null_data = df[column_name].dropna()
                        invalid_region_codes = []
                        invalid_region_code_count = 0
                        mismatched_region_codes = []
                        mismatched_region_code_count = 0

                        if len(non_null_data) > 0:
                            # Get all valid subdivision codes from pycountry
                            valid_subdivisions = set()
                            for subdivision in pycountry.subdivisions:
                                if hasattr(subdivision, 'code'):
                                    valid_subdivisions.add(subdivision.code.upper().split('-')[-1])

                            # Convert to string and strip whitespace, then to uppercase
                            non_null_data_str = non_null_data.astype(str).str.strip().str.upper()

                            # Check validity of region codes
                            not_in_valid_mask = ~non_null_data_str.isin(valid_subdivisions)

                            # Extract unique invalid region codes (original values as fetched from DB)
                            invalid_region_codes = non_null_data[not_in_valid_mask].unique().tolist()
                            invalid_region_code_count = len(invalid_region_codes)

                            # Check for region-country code mismatches
                            try:
                                # Get data for both region and country columns where both are not null
                                cursor.execute("""
                                    SELECT "{country_column_in_table}", "{column_name}"
                                    FROM "{schema}"."{table_name}"
                                    WHERE "{column_name}" IS NOT NULL AND "{country_column_in_table}" IS NOT NULL
                                """)
                                region_country_data = cursor.fetchall()

                                if region_country_data:
                                    df_region_country = pd.DataFrame(region_country_data, columns=[country_column_in_table, column_name])

                                    # Create mapping from country code to valid region codes
                                    country_to_regions = {}
                                    for subdivision in pycountry.subdivisions:
                                        if hasattr(subdivision, 'country_code') and hasattr(subdivision, 'code'):
                                            country_code = subdivision.country_code.upper()
                                            region_code = subdivision.code.upper().split('-')[-1]
                                            if country_code not in country_to_regions:
                                                country_to_regions[country_code] = set()
                                            country_to_regions[country_code].add(region_code)

                                    # Process data for mismatch checking
                                    non_null_region_country = df_region_country.dropna()

                                    if len(non_null_region_country) > 0:
                                        # Convert country values to alpha_2 codes using bulk conversion
                                        country_values = non_null_region_country[country_column_in_table].astype(str).str.strip()
                                        country_values_list = country_values.tolist()
                                        # Bulk convert using country_converter
                                        country_codes_alpha2 = cc.convert(country_values_list, to='ISO2', not_found=None)
                                        country_codes_alpha2 = pd.Series(country_codes_alpha2)

                                        # Convert to uppercase for comparison
                                        country_codes_upper = country_codes_alpha2.astype(str).str.upper()
                                        region_codes_upper = non_null_region_country[column_name].astype(str).str.strip().str.upper()

                                        # Find mismatches where region code doesn't belong to the country
                                        mismatch_records = []
                                        for idx, row in non_null_region_country.iterrows():
                                            country_code = country_codes_upper.iloc[idx]
                                            region_code = region_codes_upper.iloc[idx]

                                            # Skip if country conversion failed (None or 'NONE')
                                            if country_code and country_code != 'NONE' and country_code in country_to_regions:
                                                if region_code not in country_to_regions[country_code]:
                                                    mismatch_records.append({
                                                        column_name: row[column_name],
                                                        country_column_in_table: row[country_column_in_table]
                                                    })

                                        # Get unique mismatched combinations
                                        if mismatch_records:
                                            df_mismatches = pd.DataFrame(mismatch_records)
                                            mismatched_region_codes = (
                                                df_mismatches
                                                .drop_duplicates()
                                                .to_dict('records')
                                            )
                                            mismatched_region_code_count = len(mismatched_region_codes)

                            except Exception:
                                # If there's an error in mismatch checking, set defaults
                                mismatched_region_codes = []
                                mismatched_region_code_count = 0

                            # Get distinct regions grouped by country
                            regions_counts = df.groupby([country_column_in_table, column_name]).size()
                            distinct_regions = [
                                {
                                    "value": {
                                        country_column_in_table: str(country_val),
                                        column_name: str(region_val)
                                    },
                                    "count": int(count)
                                }
                                for (country_val, region_val), count in regions_counts.items()
                            ]

                        return Response({
                            "column_name": column_name,
                            "column_category": column_category,
                            "column_in_table": country_column_in_table,
                            "statistics": {
                                "null_count": int(null_count),
                                "total_count": len(df),
                                "distinct_regions": distinct_regions,
                                "invalid_region_category_count": invalid_region_code_count,
                                "invalid_region_categories": invalid_region_codes,
                                "mismatched_region_category_count": int(mismatched_region_code_count),
                                "mismatched_region_categories": mismatched_region_codes
                            }
                        })
                    else:
                        # If country_column_in_table doesn't exist, return response indicating it's required
                        return Response({
                            "column_name": column_name,
                            "column_category": column_category,
                            "column_in_table": None,
                            "error": "country_column_in_table is required for region_category validation"
                        }, status=400)

                elif column_category == 'postal_code':
                    # First check if country_code column exists in the table
                    country_columns = get_category_columns('country_code')
                    country_column_in_table = None
                    if country_columns:
                        country_column_in_table = check_columns_in_table(cursor, schema, table_name, country_columns)

                    # Check if country column exists
                    if country_column_in_table:
                        # Load both country_code and postal_code columns together
                        print(country_column_in_table, column_name)
                        cursor.execute("""
                            SELECT "{country_column_in_table}", "{column_name}"
                            FROM "{schema}"."{table_name}"
                        """)
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=[country_column_in_table, column_name])

                        # # Check if maximum character length in postal_code column is more than three
                        # max_char_len = df[country_column_in_table].dropna().astype(str).apply(len).max() if not df.empty else 0

                        # if max_char_len > 3:
                        #     return Response(
                        #         {"error": "Maximum character length in this column is less than three."},
                        #         status=status.HTTP_400_BAD_REQUEST
                        #     )

                        # Basic statistics
                        null_count = df[column_name].isnull().sum()
                        non_null_count = df[column_name].notna().sum()
                        total_count = len(df)

                        df_postal_country = df.dropna(subset=[country_column_in_table, column_name]).copy()
                        if len(df_postal_country) > 0:
                            postal_counts = df_postal_country.groupby([country_column_in_table, column_name]).size()
                            distinct_postal_codes = [
                                {
                                    "value": {
                                        country_column_in_table: str(country_val),
                                        column_name: str(postal_val)
                                    },
                                    "count": int(count)
                                }
                                for (country_val, postal_val), count in postal_counts.items()
                            ]

                        # Get non-null data for validation - both postal_code and country must be present
                        df_validation = df.dropna(subset=[column_name, country_column_in_table]).copy()

                        if len(df_validation) > 0:
                            # Cache for Nominatim objects
                            nomi_cache = {}

                            def get_alpha2_country(country):
                                """Convert country name or code to ISO alpha-2 code."""
                                if not country:
                                    return None
                                country = country.strip()
                                # If already 2 letters, assume it's alpha-2
                                if len(country) == 2:
                                    return country.upper()
                                try:
                                    return pycountry.countries.lookup(country).alpha_2
                                except LookupError:
                                    return None

                            def is_valid_postal(country_input, postal_code):
                                try:
                                    if postal_code is None or str(postal_code).strip() == "":
                                        return False

                                    country_code = get_alpha2_country(country_input)
                                    if not country_code:
                                        return False

                                    # Use cached Nominatim object
                                    if country_code not in nomi_cache:
                                        nomi_cache[country_code] = pgeocode.Nominatim(country_code)

                                    nomi = nomi_cache[country_code]
                                    loc = nomi.query_postal_code(str(postal_code))
                                    return not pd.isna(loc.get('country_code'))
                                except Exception:
                                    return False
                            df_validation['is_valid'] = df_validation.apply(lambda row: is_valid_postal(row[country_column_in_table], row[column_name]), axis=1)

                            # Step 5: Extract all mismatched records (invalid postal codes)
                            df_mismatched = df_validation[~df_validation['is_valid']].copy()

                            if len(df_mismatched) > 0:
                                # Get unique mismatched postal codes with their country codes
                                df_mismatched_unique = df_mismatched[[country_column_in_table, column_name]].drop_duplicates()
                                mismatched_postal_codes = df_mismatched_unique.to_dict('records')
                                mismatched_postal_code_count = len(mismatched_postal_codes)
                            else:
                                mismatched_postal_codes = []
                                mismatched_postal_code_count = 0
                        else:
                            # No non-null data to validate
                            mismatched_postal_codes = []
                            mismatched_postal_code_count = 0

                    else:
                        # No country_code column exists - cannot validate without country
                        return Response({
                            "error": "country_code column required for postal_code validation"
                        }, status=status.HTTP_400_BAD_REQUEST)

                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "column_in_table": country_column_in_table,
                        "statistics": {
                            "null_count": int(null_count),
                            "non_null_count": int(non_null_count),
                            "total_count": int(total_count),
                            "distinct_postal_codes": distinct_postal_codes,
                            "mismatched_postal_code_count": int(mismatched_postal_code_count),
                            "mismatched_postal_codes": mismatched_postal_codes
                        }
                    })

                elif column_category == 'units':
                    # Load data using pandas for better unit analysis
                    cursor.execute("""
                        SELECT "{column_name}"
                        FROM "{schema}"."{table_name}"
                    """)

                    # Fetch all data and create DataFrame
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=[column_name])

                    # Basic statistics
                    null_count = df[column_name].isnull().sum()

                    # Get distinct units and their counts
                    unit_counts = df[column_name].dropna().astype(str).str.strip().value_counts()
                    distinct_values = [
                        {"value": str(unit), "count": int(count)}
                        for unit, count in unit_counts.items()
                    ]

                    # Validate units using pint
                    invalid_units = []

                    non_null_data = df[column_name].dropna()
                    if len(non_null_data) > 0:
                        # Create UnitRegistry instance
                        ureg = pint.UnitRegistry()

                        # Convert to string and strip whitespace
                        non_null_data_str = non_null_data.astype(str).str.strip()

                        # Function to validate a unit string (case-insensitive)
                        def is_valid_unit(unit_str):
                            """Validate if a unit string is a valid pint unit expression"""
                            try:
                                # Try to parse the expression in lowercase for validation
                                ureg.parse_expression(unit_str.lower())
                                return True
                            except Exception:
                                return False

                        # Apply validation to each unit
                        is_valid_mask = non_null_data_str.apply(is_valid_unit)

                        # Get invalid units (preserve original case)
                        invalid_mask = ~is_valid_mask
                        invalid_units = non_null_data_str[invalid_mask].unique().tolist()

                    return Response({
                        "column_name": column_name,
                        "column_category": column_category,
                        "statistics": {
                            "null_count": int(null_count),
                            "distinct_values": distinct_values,
                            "invalid_units": invalid_units
                        }
                    })
                # else:
                #     cursor.execute("""
                #         SELECT "{column_name}"
                #         FROM "{schema}"."{table_name}"
                #     """)

                #     # Fetch all data and create DataFrame
                #     data = cursor.fetchall()
                #     df = pd.DataFrame(data, columns=[column_name])

                #     # Basic statistics
                #     null_count = df[column_name].isnull().sum()
                #     category = get_column_info(column_name)

        except Exception as e:
            return Response({
                "error": f"error: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class ColumnSequenceListView(APIView):
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            table_name = request.query_params.get('table_name')
            user = request.user
            customer = user.cust_id
            if not customer:
                return Response(
                    {"error": "User is not associated with any customer."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if not table_name:
                return Response(
                    {"error": "table_name is required."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            customer_connection = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                database=customer.cust_db,
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD']
            )
            customer_connection.autocommit = True
            with customer_connection.cursor() as cursor:
                sql = """
                    SELECT seq_name, username, scope
                    FROM "GENERAL"."tbl_col_seq"
                    WHERE table_name = %s
                    AND (
                        username = %s
                        OR scope = 'G'
                    )
                    ORDER BY seq_name
                """
                cursor.execute(sql, (table_name, user.email))
                rows = cursor.fetchall()
                result = [
                    {
                        "seq_name": row[0],
                        "username": row[1],
                        "scope": row[2],
                        "is_owner": row[1] == user.email or row[2] == 'G'  # Global sequences can be edited by anyone
                    }
                    for row in rows
                ]
            customer_connection.close()
            return Response({"sequences": result})
        except Exception as e:
            return Response({
                "error": f"error: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class ColumnSequenceView(APIView):
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            table_name = request.query_params.get('table_name')
            user = request.user
            customer = user.cust_id
            if not customer:
                return Response(
                    {"error": "User is not associated with any customer."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if not table_name:
                return Response(
                    {"error": "table_name and schema are required."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            customer_connection = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                database=customer.cust_db,
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD']
            )
            customer_connection.autocommit = True
            with customer_connection.cursor() as cursor:
                # Fetch user-specific sequences and global ones for table
                sql = """
                    SELECT seq_name, sequence
                    FROM "GENERAL"."tbl_col_seq"
                    WHERE table_name = %s
                    AND (
                        username = %s
                        OR scope = 'G'
                    )
                    ORDER BY
                        seq_name
                """
                cursor.execute(sql, (table_name, user.email))
                rows = cursor.fetchall()
                result = [
                    {
                        "seq_name": row[0],
                        "sequence": row[1],
                    }
                    for row in rows
                ]
            customer_connection.close()
            return Response({"sequences": result})
        except Exception as e:
            return Response({
                "error": f"error: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def post(self, request):
        """Create a new column sequence"""
        try:
            table_name = request.data.get('table_name')
            sequence = request.data.get('sequence')
            seq_name = request.data.get('seq_name')
            scope = request.data.get('scope', 'L')  # Default to Local scope
            user = request.user
            customer = user.cust_id

            if not customer:
                return Response(
                    {"error": "User is not associated with any customer."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if not table_name or not sequence or not seq_name:
                return Response(
                    {"error": "table_name, sequence, and seq_name are required."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            customer_connection = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                database=customer.cust_db,
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD']
            )
            customer_connection.autocommit = True

            with customer_connection.cursor() as cursor:
                # Check if sequence name already exists for this table and user
                check_sql = """
                    SELECT seq_name FROM "GENERAL"."tbl_col_seq"
                    WHERE table_name = %s AND seq_name = %s AND username = %s
                """
                cursor.execute(check_sql, (table_name, seq_name, user.email))
                existing = cursor.fetchone()

                if existing:
                    customer_connection.close()
                    return Response(
                        {"error": f"Sequence name '{seq_name}' already exists for this table."},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                # Insert new sequence
                sql = """
                    INSERT INTO "GENERAL"."tbl_col_seq" (username, table_name, sequence, seq_name, scope)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (user.email, table_name, sequence, seq_name, scope))

            customer_connection.close()
            return Response({"message": f"Sequence '{seq_name}' created successfully"})
        except Exception as e:
            return Response({
                "error": f"Failed to create sequence: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def put(self, request):
        """Update an existing column sequence"""
        try:
            table_name = request.data.get('table_name')
            sequence = request.data.get('sequence')
            seq_name = request.data.get('seq_name')
            old_seq_name = request.data.get('old_seq_name')  # For renaming
            scope = request.data.get('scope', 'L')
            user = request.user
            customer = user.cust_id

            if not customer:
                return Response(
                    {"error": "User is not associated with any customer."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if not table_name or not sequence or not seq_name:
                return Response(
                    {"error": "table_name, sequence, and seq_name are required."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            customer_connection = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                database=customer.cust_db,
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD']
            )
            customer_connection.autocommit = True

            with customer_connection.cursor() as cursor:
                # Use old_seq_name if provided (for renaming), otherwise use seq_name
                update_seq_name = old_seq_name if old_seq_name else seq_name

                # Check if user owns this sequence or if it's a global sequence
                check_sql = """
                    SELECT username, scope FROM "GENERAL"."tbl_col_seq"
                    WHERE table_name = %s AND seq_name = %s
                """
                cursor.execute(check_sql, (table_name, update_seq_name))
                result = cursor.fetchone()

                if not result:
                    customer_connection.close()
                    return Response(
                        {"error": f"Sequence '{update_seq_name}' not found."},
                        status=status.HTTP_404_NOT_FOUND
                    )

                # Allow editing if user owns the sequence OR if it's a global sequence
                if result[0] != user.email and result[1] != 'G':
                    customer_connection.close()
                    return Response(
                        {"error": "You don't have permission to edit this sequence."},
                        status=status.HTTP_403_FORBIDDEN
                    )

                # If renaming, check if new name already exists
                if old_seq_name and old_seq_name != seq_name:
                    cursor.execute(check_sql, (table_name, seq_name))
                    if cursor.fetchone():
                        customer_connection.close()
                        return Response(
                            {"error": f"Sequence name '{seq_name}' already exists."},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                # Update sequence (keep original username if updating global sequence)
                update_sql = """
                    UPDATE "GENERAL"."tbl_col_seq"
                    SET sequence = %s, seq_name = %s, scope = %s
                    WHERE table_name = %s AND seq_name = %s
                """
                cursor.execute(update_sql, (sequence, seq_name, scope, table_name, update_seq_name))

            customer_connection.close()
            return Response({"message": f"Sequence '{seq_name}' updated successfully"})
        except Exception as e:
            return Response({
                "error": f"Failed to update sequence: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request):
        """Delete a column sequence"""
        try:
            table_name = request.data.get('table_name')
            seq_name = request.data.get('seq_name')
            user = request.user
            customer = user.cust_id

            if not customer:
                return Response(
                    {"error": "User is not associated with any customer."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            if not table_name or not seq_name:
                return Response(
                    {"error": "table_name and seq_name are required."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            customer_connection = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                database=customer.cust_db,
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD']
            )
            customer_connection.autocommit = True

            with customer_connection.cursor() as cursor:
                # Check if user owns this sequence or if it's a global sequence
                check_sql = """
                    SELECT username, scope FROM "GENERAL"."tbl_col_seq"
                    WHERE table_name = %s AND seq_name = %s
                """
                cursor.execute(check_sql, (table_name, seq_name))
                result = cursor.fetchone()

                if not result:
                    customer_connection.close()
                    return Response(
                        {"error": f"Sequence '{seq_name}' not found."},
                        status=status.HTTP_404_NOT_FOUND
                    )

                # Allow deleting if user owns the sequence OR if it's a global sequence
                if result[0] != user.email and result[1] != 'G':
                    customer_connection.close()
                    return Response(
                        {"error": "You don't have permission to delete this sequence."},
                        status=status.HTTP_403_FORBIDDEN
                    )

                # Delete sequence
                delete_sql = """
                    DELETE FROM "GENERAL"."tbl_col_seq"
                    WHERE table_name = %s AND seq_name = %s
                """
                cursor.execute(delete_sql, (table_name, seq_name))

            customer_connection.close()
            return Response({"message": f"Sequence '{seq_name}' deleted successfully"})
        except Exception as e:
            return Response({
                "error": f"Failed to delete sequence: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ============================================================
# From: expression_testing.py
# ============================================================
"""
Expression Testing API for Calculated Columns

Executes test cases against calculated column expressions to validate runtime behavior.
"""

import logging
import re
from typing import Any, Optional

from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication

logger = logging.getLogger(__name__)

class ExpressionTestEngine:
    """Executes test cases against SQL expressions"""

    def __init__(self, expression: str, available_columns: list[dict[str, Any]]):
        self.expression = expression
        self.available_columns = {col['name']: col.get('datatype', 'TEXT') for col in available_columns}

    def apply_null_safety(self, expr: str) -> str:
        """Apply NULL safety rules to string functions"""
        # Wrap string function arguments with COALESCE
        # UPPER(col) -> UPPER(COALESCE(col, ''))
        # LOWER(col) -> LOWER(COALESCE(col, ''))
        # CONCAT(a, b) -> CONCAT(COALESCE(a, ''), COALESCE(b, ''))

        empty_str = "''"

        # Handle CONCAT
        def replace_concat(match):
            args = match.group(1).split(',')
            coalesced_args = [f'COALESCE({arg.strip()}, {empty_str})' for arg in args]
            return f"CONCAT({', '.join(coalesced_args)})"

        expr = re.sub(
            r'CONCAT\s*\(([^)]+)\)',
            replace_concat,
            expr,
            flags=re.IGNORECASE
        )

        # Handle UPPER
        def replace_upper(match):
            return f"UPPER(COALESCE({match.group(1).strip()}, {empty_str}))"

        expr = re.sub(
            r'UPPER\s*\(([^)]+)\)',
            replace_upper,
            expr,
            flags=re.IGNORECASE
        )

        # Handle LOWER
        def replace_lower(match):
            return f"LOWER(COALESCE({match.group(1).strip()}, {empty_str}))"

        expr = re.sub(
            r'LOWER\s*\(([^)]+)\)',
            replace_lower,
            expr,
            flags=re.IGNORECASE
        )

        # Handle SUBSTRING
        def replace_substring(match):
            arg1 = match.group(1).strip()
            arg2 = match.group(2).strip()
            arg3 = match.group(3).strip() if match.group(3) else None
            if arg3:
                return f"SUBSTRING(COALESCE({arg1}, {empty_str}), {arg2}, {arg3})"
            else:
                return f"SUBSTRING(COALESCE({arg1}, {empty_str}), {arg2})"

        expr = re.sub(
            r'SUBSTRING\s*\(([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)',
            replace_substring,
            expr,
            flags=re.IGNORECASE
        )

        return expr

    def evaluate_test(self, test_input: dict[str, Any]) -> dict[str, Any]:
        """
        Evaluate expression with test input row

        Returns:
        {
            "success": bool,
            "result": any,
            "error": str or None,
            "sql": str  # Generated SQL for debugging
        }
        """
        try:
            # IMPORTANT: Replace column references FIRST, then apply NULL safety
            # This ensures column names are replaced before they get wrapped in COALESCE
            sql_expression = self.expression

            # Replace column references with test values
            # Test input values always take precedence - replace all column names in test_input
            for col_name, col_value in test_input.items():
                # Replace column name with value (test values take precedence)
                if col_value is None:
                    sql_value = "NULL"
                elif isinstance(col_value, str):
                    # Escape single quotes by doubling them
                    escaped_value = col_value.replace("'", "''")
                    sql_value = f"'{escaped_value}'"
                elif isinstance(col_value, (int, float)):
                    sql_value = str(col_value)
                elif isinstance(col_value, bool):
                    sql_value = "TRUE" if col_value else "FALSE"
                else:
                    escaped_str = str(col_value).replace("'", "''")
                    sql_value = f"'{escaped_str}'"

                # Replace column references
                # Handle multiple formats: regular names, bracketed [name], and quoted "name"
                escaped_col_name = re.escape(col_name)

                # Replace bracketed column names: [column name]
                sql_expression = re.sub(
                    rf'\[{escaped_col_name}\]',
                    sql_value,
                    sql_expression,
                    flags=re.IGNORECASE
                )

                # Replace quoted column names: "column name" or 'column name'
                sql_expression = re.sub(
                    r'["\']{escaped_col_name}["\']',
                    sql_value,
                    sql_expression,
                    flags=re.IGNORECASE
                )

                # Replace regular column names (word boundaries to avoid partial matches)
                # Use case-insensitive replacement to handle case variations
                sql_expression = re.sub(
                    rf'\b{escaped_col_name}\b',
                    sql_value,
                    sql_expression,
                    flags=re.IGNORECASE
                )

            # Now apply NULL safety (wraps string function arguments with COALESCE)
            safe_expression = self.apply_null_safety(sql_expression)
            sql_expression = safe_expression

            # Evaluate with nested function support and intermediate logging
            result, evaluation_steps = self._simulate_evaluation_with_steps(sql_expression, test_input)

            return {
                "success": True,
                "result": result,
                "error": None,
                "sql": sql_expression,
                "debug_steps": evaluation_steps
            }

        except Exception as e:
            logger.error(f"Error evaluating test: {e!s}", exc_info=True)
            return {
                "success": False,
                "result": None,
                "error": str(e),
                "sql": None
            }

    def _simulate_evaluation_with_steps(self, sql_expr: str, test_input: dict[str, Any]) -> tuple:
        """
        Simulate SQL expression evaluation with intermediate step logging for nested functions.
        Returns (result, debug_steps) where debug_steps contains each function evaluation.
        """
        debug_steps = []
        result = self._simulate_evaluation_recursive(sql_expr, test_input, debug_steps, depth=0)
        return result, debug_steps

    def _simulate_evaluation_recursive(self, sql_expr: str, test_input: dict[str, Any], debug_steps: list, depth: int) -> Any:
        """
        Recursively evaluate nested functions, logging each step.
        """
        import re

        def extract_nested_parens(expr: str, start_pos: int) -> tuple:
            """Extract content between parentheses, handling nesting"""
            if start_pos >= len(expr) or expr[start_pos] != '(':
                return None, start_pos
            start_pos += 1
            content = ''
            depth = 1
            pos = start_pos
            while pos < len(expr) and depth > 0:
                if expr[pos] == '(':
                    depth += 1
                elif expr[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        return content, pos + 1
                content += expr[pos]
                pos += 1
            return content, pos

        def extract_value_from_coalesce(expr: str) -> str:
            """Extract the actual value from COALESCE(arg, '') or just return the value"""
            coalesce_pattern = r"COALESCE\s*\(([^,]+),\s*''\)"
            match = re.search(coalesce_pattern, expr, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            return expr.strip()

        def get_string_value(value_str: str) -> str:
            """Extract string value, handling quotes and NULL"""
            value_str = value_str.strip()
            if (value_str.startswith("'") and value_str.endswith("'")) or \
               (value_str.startswith('"') and value_str.endswith('"')):
                value_str = value_str[1:-1]
            value_str = value_str.replace("''", "'")
            if value_str == 'NULL':
                return ''
            return value_str

        # Check for nested functions - process innermost first
        # Look for function calls that contain other function calls

        # Handle SUBSTRING(UPPER(...), ...) - nested case
        substring_upper_match = re.search(r'SUBSTRING\s*\(\s*UPPER\s*\(', sql_expr, re.IGNORECASE)
        if substring_upper_match:
            # Extract the full SUBSTRING call
            start_pos = substring_upper_match.start()
            args_expr, end_pos = extract_nested_parens(sql_expr, start_pos + len('SUBSTRING'))
            if args_expr:
                # Split arguments
                parts = []
                current_part = ''
                paren_depth = 0
                for char in args_expr:
                    if char == '(':
                        paren_depth += 1
                        current_part += char
                    elif char == ')':
                        paren_depth -= 1
                        current_part += char
                    elif char == ',' and paren_depth == 0:
                        parts.append(current_part.strip())
                        current_part = ''
                    else:
                        current_part += char
                if current_part:
                    parts.append(current_part.strip())

                if parts and parts[0].upper().startswith('UPPER'):
                    # Evaluate UPPER first
                    upper_arg_expr, _ = extract_nested_parens(parts[0], parts[0].index('('))
                    upper_arg = extract_value_from_coalesce(upper_arg_expr)
                    upper_input = get_string_value(upper_arg)
                    upper_output = upper_input.upper()

                    debug_steps.append({
                        'stage': 'UPPER',
                        'input': upper_input,
                        'output': upper_output,
                        'depth': depth + 1
                    })

                    # Now evaluate SUBSTRING with UPPER result
                    start = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
                    length = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                    if length:
                        substring_output = upper_output[start-1:start-1+length] if start > 0 and start <= len(upper_output) else ''
                    else:
                        substring_output = upper_output[start-1:] if start > 0 and start <= len(upper_output) else ''

                    debug_steps.append({
                        'stage': 'SUBSTRING',
                        'input': upper_output,
                        'output': substring_output,
                        'args': parts[1:],
                        'depth': depth
                    })

                    return substring_output

        # Handle UPPER(COALESCE(...))
        upper_match = re.search(r'UPPER\s*\(', sql_expr, re.IGNORECASE)
        if upper_match:
            start_pos = upper_match.end()
            arg_expr, _ = extract_nested_parens(sql_expr, start_pos - 1)
            if arg_expr:
                arg = extract_value_from_coalesce(arg_expr)
                arg = get_string_value(arg)
                result = arg.upper()
                debug_steps.append({
                    'stage': 'UPPER',
                    'input': arg,
                    'output': result,
                    'depth': depth
                })
                return result

        # Handle LOWER(COALESCE(...))
        lower_match = re.search(r'LOWER\s*\(', sql_expr, re.IGNORECASE)
        if lower_match:
            start_pos = lower_match.end()
            arg_expr, _ = extract_nested_parens(sql_expr, start_pos - 1)
            if arg_expr:
                arg = extract_value_from_coalesce(arg_expr)
                arg = get_string_value(arg)
                result = arg.lower()
                debug_steps.append({
                    'stage': 'LOWER',
                    'input': arg,
                    'output': result,
                    'depth': depth
                })
                return result

        # Handle CONCAT(COALESCE(...), COALESCE(...))
        concat_pattern = r'CONCAT\s*\((.*?)\)'
        concat_match = re.search(concat_pattern, sql_expr, re.IGNORECASE | re.DOTALL)
        if concat_match:
            args_str = concat_match.group(1)
            # Split by comma, handling nested parentheses
            args = []
            current_arg = ''
            paren_depth = 0
            for char in args_str:
                if char == '(':
                    paren_depth += 1
                    current_arg += char
                elif char == ')':
                    paren_depth -= 1
                    current_arg += char
                elif char == ',' and paren_depth == 0:
                    args.append(current_arg.strip())
                    current_arg = ''
                else:
                    current_arg += char
            if current_arg:
                args.append(current_arg.strip())

            # Extract values from each COALESCE
            values = []
            for arg in args:
                value = extract_value_from_coalesce(arg)
                value = get_string_value(value)
                values.append(value)

            result = ''.join(values)
            debug_steps.append({
                'stage': 'CONCAT',
                'input': values,
                'output': result,
                'depth': depth
            })
            return result

        # Handle SUBSTRING(COALESCE(...), start, length)
        substring_pattern = r'SUBSTRING\s*\(([^,]+),\s*(\d+)(?:,\s*(\d+))?\)'
        substring_match = re.search(substring_pattern, sql_expr, re.IGNORECASE)
        if substring_match:
            arg_expr = substring_match.group(1).strip()
            start = int(substring_match.group(2))
            length = int(substring_match.group(3)) if substring_match.group(3) else None
            arg = extract_value_from_coalesce(arg_expr)
            arg = get_string_value(arg)
            if not arg:
                result = ''
            else:
                # SQL SUBSTRING is 1-indexed
                if length:
                    result = arg[start-1:start-1+length] if start > 0 and start <= len(arg) else ''
                else:
                    result = arg[start-1:] if start > 0 and start <= len(arg) else ''

            debug_steps.append({
                'stage': 'SUBSTRING',
                'input': arg,
                'output': result,
                'args': [str(start)] + ([str(length)] if length else []),
                'depth': depth
            })
            return result

        # Default: return the expression as-is (for simple column references)
        result = sql_expr.strip()
        if result.startswith("'") and result.endswith("'"):
            return result[1:-1].replace("''", "'")
        return result

    def _simulate_evaluation(self, sql_expr: str, test_input: dict[str, Any]) -> Any:
        """Legacy method - use _simulate_evaluation_with_steps for nested function support"""
        result, _ = self._simulate_evaluation_with_steps(sql_expr, test_input)
        return result
        """Simulate SQL expression evaluation (for testing without database)"""
        # This is a simplified simulation - in production, execute against test DB

        def extract_value_from_coalesce(expr: str) -> str:
            """Extract the actual value from COALESCE(arg, '') or just return the value"""
            # Match COALESCE(arg, '')
            coalesce_pattern = r"COALESCE\s*\(([^,]+),\s*''\)"
            match = re.search(coalesce_pattern, expr, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            return expr.strip()

        def get_string_value(value_str: str) -> str:
            """Extract string value, handling quotes and NULL"""
            value_str = value_str.strip()
            # Remove surrounding quotes
            if (value_str.startswith("'") and value_str.endswith("'")) or \
               (value_str.startswith('"') and value_str.endswith('"')):
                value_str = value_str[1:-1]
            # Unescape doubled quotes
            value_str = value_str.replace("''", "'")
            if value_str == 'NULL':
                return ''
            return value_str

        def extract_nested_parens(expr: str, start_pos: int) -> tuple:
            """Extract content between parentheses, handling nesting"""
            if start_pos >= len(expr) or expr[start_pos] != '(':
                return None, start_pos
            start_pos += 1
            content = ''
            depth = 1
            pos = start_pos
            while pos < len(expr) and depth > 0:
                if expr[pos] == '(':
                    depth += 1
                elif expr[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        return content, pos + 1
                content += expr[pos]
                pos += 1
            return content, pos

        # Handle UPPER(COALESCE(...))
        upper_match = re.search(r'UPPER\s*\(', sql_expr, re.IGNORECASE)
        if upper_match:
            start_pos = upper_match.end()
            arg_expr, _ = extract_nested_parens(sql_expr, start_pos - 1)
            if arg_expr:
                arg = extract_value_from_coalesce(arg_expr)
                arg = get_string_value(arg)
                result = arg.upper()
                logger.debug(f"[Test Eval] UPPER: input='{arg}' -> output='{result}'")
                return result

        # Handle LOWER(COALESCE(...))
        lower_match = re.search(r'LOWER\s*\(', sql_expr, re.IGNORECASE)
        if lower_match:
            start_pos = lower_match.end()
            arg_expr, _ = extract_nested_parens(sql_expr, start_pos - 1)
            if arg_expr:
                arg = extract_value_from_coalesce(arg_expr)
                arg = get_string_value(arg)
                return arg.lower()

        # Handle CONCAT(COALESCE(...), COALESCE(...))
        concat_pattern = r'CONCAT\s*\((.*?)\)'
        concat_match = re.search(concat_pattern, sql_expr, re.IGNORECASE | re.DOTALL)
        if concat_match:
            args_str = concat_match.group(1)
            # Split by comma, handling nested parentheses
            args = []
            current_arg = ''
            paren_depth = 0
            for char in args_str:
                if char == '(':
                    paren_depth += 1
                    current_arg += char
                elif char == ')':
                    paren_depth -= 1
                    current_arg += char
                elif char == ',' and paren_depth == 0:
                    args.append(current_arg.strip())
                    current_arg = ''
                else:
                    current_arg += char
            if current_arg:
                args.append(current_arg.strip())

            # Extract values from each COALESCE
            values = []
            for arg in args:
                value = extract_value_from_coalesce(arg)
                value = get_string_value(value)
                values.append(value)
            return ''.join(values)

        # Handle SUBSTRING(COALESCE(...), start, length)
        # Need to handle COALESCE wrapping - extract the full first argument including nested functions
        substring_match = re.search(r'SUBSTRING\s*\(', sql_expr, re.IGNORECASE)
        if substring_match:
            start_pos = substring_match.end()
            args_expr, end_pos = extract_nested_parens(sql_expr, start_pos - 1)
            if args_expr:
                # Split arguments (handling nested parentheses and COALESCE)
                parts = []
                current_part = ''
                paren_depth = 0
                for char in args_expr:
                    if char == '(':
                        paren_depth += 1
                        current_part += char
                    elif char == ')':
                        paren_depth -= 1
                        current_part += char
                    elif char == ',' and paren_depth == 0:
                        parts.append(current_part.strip())
                        current_part = ''
                    else:
                        current_part += char
                if current_part.strip():
                    parts.append(current_part.strip())

                if parts:
                    # First argument may be wrapped in COALESCE
                    first_arg = parts[0]
                    arg = extract_value_from_coalesce(first_arg)
                    arg = get_string_value(arg)

                    # Get start position (second argument)
                    start = int(parts[1]) if len(parts) > 1 and parts[1].strip().isdigit() else 1
                    # Get length (third argument, optional)
                    length = int(parts[2]) if len(parts) > 2 and parts[2].strip().isdigit() else None

                    if not arg:
                        result = ''
                    else:
                        # SQL SUBSTRING is 1-indexed
                        if length:
                            result = arg[start-1:start-1+length] if start > 0 and start <= len(arg) else ''
                        else:
                            result = arg[start-1:] if start > 0 and start <= len(arg) else ''

                    return result

        # Default: return the expression as-is (for simple column references)
        result = sql_expr.strip()
        if result.startswith("'") and result.endswith("'"):
            return result[1:-1].replace("''", "'")
        return result

class TestExpressionView(APIView):
    """API endpoint for testing calculated column expressions with test cases"""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Test a calculated column expression with test cases

        Request body:
        {
            "expression": "UPPER(firstname)",
            "available_columns": [
                {"name": "firstname", "datatype": "TEXT"}
            ],
            "test_cases": [
                {
                    "input": {"firstname": "abc"},
                    "expected": "ABC",
                    "description": "UPPER converts to uppercase"
                }
            ]
        }

        Response:
        {
            "success": true,
            "results": [
                {
                    "test": {...},
                    "passed": true/false,
                    "actual": "ABC",
                    "error": null
                }
            ]
        }
        """
        try:
            expression = request.data.get('expression', '').strip()
            available_columns = request.data.get('available_columns', [])
            test_cases = request.data.get('test_cases', [])

            if not expression:
                return Response(
                    {"success": False, "error": "Expression is required", "results": []},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not test_cases:
                return Response(
                    {"success": False, "error": "At least one test case is required", "results": []},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Create test engine
            engine = ExpressionTestEngine(expression, available_columns)

            # Execute all test cases
            results = []
            for test_case in test_cases:
                test_input = test_case.get('input', {})
                expected = test_case.get('expected')
                description = test_case.get('description', 'Test case')

                # Evaluate
                eval_result = engine.evaluate_test(test_input)

                if eval_result['success']:
                    actual = eval_result['result']
                    # Normalize for comparison
                    actual_str = '' if actual is None else str(actual)
                    expected_str = '' if expected is None else str(expected)

                    passed = actual_str == expected_str
                    results.append({
                        "test": {
                            "input": test_input,
                            "expected": expected,
                            "description": description
                        },
                        "passed": passed,
                        "actual": actual,
                        "error": None,
                        "sql": eval_result.get('sql'),
                        "debug_steps": eval_result.get('debug_steps', [])  # Include intermediate evaluation steps
                    })
                else:
                    results.append({
                        "test": {
                            "input": test_input,
                            "expected": expected,
                            "description": description
                        },
                        "passed": False,
                        "actual": None,
                        "error": eval_result.get('error', 'Evaluation failed'),
                        "sql": eval_result.get('sql')
                    })

            return Response({
                "success": True,
                "results": results
            })

        except Exception as e:
            logger.error(f"Error in TestExpressionView: {e!s}", exc_info=True)
            return Response(
                {"success": False, "error": str(e), "results": []},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# ============================================================
# From: expression_validation.py
# ============================================================
"""
Expression Validation API for Calculated Columns

Validates SQL expressions used in calculated columns, checking:
- Column references exist
- Functions are supported
- Syntax is correct
- Types are compatible
"""

import logging
from typing import Any

from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication

logger = logging.getLogger(__name__)

# Supported functions for calculated columns
SUPPORTED_FUNCTIONS = {
    # String functions
    'CONCAT': {'type': 'string', 'min_args': 2, 'max_args': None},
    'UPPER': {'type': 'string', 'min_args': 1, 'max_args': 1},
    'LOWER': {'type': 'string', 'min_args': 1, 'max_args': 1},
    'SUBSTRING': {'type': 'string', 'min_args': 2, 'max_args': 3},
    'TRIM': {'type': 'string', 'min_args': 1, 'max_args': 1},
    'LENGTH': {'type': 'integer', 'min_args': 1, 'max_args': 1},
    'REPLACE': {'type': 'string', 'min_args': 3, 'max_args': 3},

    # Numeric functions
    'SUM': {'type': 'numeric', 'min_args': 1, 'max_args': 1},
    'AVG': {'type': 'numeric', 'min_args': 1, 'max_args': 1},
    'COUNT': {'type': 'integer', 'min_args': 1, 'max_args': 1},
    'MAX': {'type': 'numeric', 'min_args': 1, 'max_args': 1},
    'MIN': {'type': 'numeric', 'min_args': 1, 'max_args': 1},
    'ROUND': {'type': 'numeric', 'min_args': 1, 'max_args': 2},
    'ABS': {'type': 'numeric', 'min_args': 1, 'max_args': 1},
    'CEIL': {'type': 'numeric', 'min_args': 1, 'max_args': 1},
    'FLOOR': {'type': 'numeric', 'min_args': 1, 'max_args': 1},

    # Date functions
    'NOW': {'type': 'date', 'min_args': 0, 'max_args': 0},
    'CURRENT_DATE': {'type': 'date', 'min_args': 0, 'max_args': 0},
    'CURRENT_TIMESTAMP': {'type': 'date', 'min_args': 0, 'max_args': 0},
    'DATE_PART': {'type': 'numeric', 'min_args': 2, 'max_args': 2},
    'EXTRACT': {'type': 'numeric', 'min_args': 2, 'max_args': 2},
    'TO_DATE': {'type': 'date', 'min_args': 2, 'max_args': 2},
    'DATE_TRUNC': {'type': 'date', 'min_args': 2, 'max_args': 2},

    # Type conversion
    'CAST': {'type': 'any', 'min_args': 2, 'max_args': 2},
    'TO_CHAR': {'type': 'string', 'min_args': 1, 'max_args': 2},
    'TO_NUMBER': {'type': 'numeric', 'min_args': 1, 'max_args': 2},

    # Conditional
    'CASE': {'type': 'any', 'min_args': 3, 'max_args': None},
    'COALESCE': {'type': 'any', 'min_args': 2, 'max_args': None},
    'NULLIF': {'type': 'any', 'min_args': 2, 'max_args': 2},
}

# SQL operators
OPERATORS = ['+', '-', '*', '/', '=', '!=', '<>', '<', '>', '<=', '>=', 'AND', 'OR', 'NOT', 'LIKE', 'ILIKE', 'IN', 'IS', 'IS NOT']

# SQL keywords/type names that should not be validated as column identifiers
SQL_NON_COLUMN_TOKENS = {
    'AS', 'NULL', 'TRUE', 'FALSE',
    'VARCHAR', 'CHAR', 'TEXT', 'STRING',
    'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
    'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL',
    'DATE', 'TIME', 'TIMESTAMP', 'DATETIME', 'BOOLEAN', 'BOOL',
}

class ExpressionValidator:
    """Validates SQL expressions for calculated columns"""

    def __init__(self, expression: str, available_columns: list[dict[str, Any]], expected_data_type: Optional[str] = None):
        self.expression = expression.strip()
        # Build a lookup that accepts name/business_name/technical_name/db_name aliases.
        self.available_columns = {}
        for col in available_columns:
            if isinstance(col, dict):
                aliases = [
                    col.get('name'),
                    col.get('business_name'),
                    col.get('technical_name'),
                    col.get('db_name'),
                    col.get('column_name'),
                ]
                for alias in aliases:
                    if alias is None:
                        continue
                    alias_str = str(alias).strip()
                    if alias_str:
                        self.available_columns[alias_str] = col
            else:
                key = str(col).strip()
                if key:
                    self.available_columns[key] = col
        self.expected_data_type = expected_data_type
        self.errors: list[str] = []
        self.inferred_type: Optional[str] = None

    def validate(self) -> dict[str, Any]:
        """Main validation method"""
        if not self.expression:
            self.errors.append("Expression cannot be empty")
            return self._build_response()

        # Step 1: Basic syntax check (parentheses, quotes)
        if not self._check_basic_syntax():
            return self._build_response()

        # Step 2: Tokenize and validate tokens
        tokens = self._tokenize()
        if not tokens:
            return self._build_response()

        # Step 3: Validate column references
        self._validate_columns(tokens)

        # Step 4: Validate functions (including CONCAT signature)
        self._validate_functions(tokens)

        # Step 5: Validate operator type compatibility
        self._validate_operators(tokens)

        # Step 6: Type inference
        self._infer_type(tokens)

        # Step 7: Type compatibility check
        if self.expected_data_type and self.inferred_type:
            self._check_type_compatibility()

        return self._build_response()

    def _check_basic_syntax(self) -> bool:
        """Check basic syntax: balanced parentheses, quotes"""
        # Check parentheses
        paren_count = 0
        for char in self.expression:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
                if paren_count < 0:
                    self.errors.append("Unmatched closing parenthesis")
                    return False

        if paren_count != 0:
            self.errors.append("Unmatched opening parenthesis")
            return False

        # Check quotes (basic check)
        single_quotes = self.expression.count("'")
        if single_quotes % 2 != 0:
            self.errors.append("Unmatched single quotes")
            return False

        return True

    def _tokenize(self) -> list[str]:
        """Tokenize the expression into identifiers, operators, functions, literals"""
        # Simple tokenization - split by whitespace and operators
        # This is a simplified version; a full SQL parser would be more complex
        tokens = []
        current_token = ""
        i = 0

        while i < len(self.expression):
            char = self.expression[i]

            # Handle whitespace
            if char.isspace():
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
                i += 1
                continue

            # Handle operators
            if char in ['+', '-', '*', '/', '=', '<', '>', '!', '(', ')', ',']:
                if current_token:
                    tokens.append(current_token)
                    current_token = ""

                # Check for multi-character operators
                if i + 1 < len(self.expression):
                    two_char = char + self.expression[i + 1]
                    if two_char in ['<=', '>=', '!=', '<>']:
                        tokens.append(two_char)
                        i += 2
                        continue

                tokens.append(char)
                i += 1
                continue

            # Handle quoted strings
            if char == "'":
                if current_token:
                    tokens.append(current_token)
                    current_token = ""

                # Find closing quote
                end_quote = self.expression.find("'", i + 1)
                if end_quote == -1:
                    self.errors.append("Unclosed string literal")
                    return []

                tokens.append(self.expression[i:end_quote + 1])
                i = end_quote + 1
                continue

            current_token += char
            i += 1

        if current_token:
            tokens.append(current_token)

        return tokens

    def _validate_columns(self, tokens: list[str]) -> None:
        """Validate that all column references exist"""
        # Extract potential column names (identifiers that aren't functions, operators, or literals)
        for token in tokens:
            # Skip operators, functions, literals
            if token in OPERATORS or token.upper() in SUPPORTED_FUNCTIONS:
                continue
            if token.upper() in SQL_NON_COLUMN_TOKENS:
                continue

            # Skip string literals
            if token.startswith("'") and token.endswith("'"):
                continue

            # Skip numeric literals
            if token.replace('.', '').replace('-', '').isdigit():
                continue

            # Skip parentheses and commas
            if token in ['(', ')', ',']:
                continue

            # This might be a column reference
            # Check if it exists (case-insensitive)
            column_found = False
            for col_name in self.available_columns.keys():
                col_name_str = col_name if isinstance(col_name, str) else str(col_name)
                if token.upper() == col_name_str.upper() or token == col_name_str:
                    column_found = True
                    break

            if not column_found:
                # Check if it's a table-prefixed column (e.g., "table.column")
                if '.' in token:
                    parts = token.split('.')
                    if len(parts) == 2:
                        col_part = parts[1]
                        for col_name in self.available_columns.keys():
                            col_name_str = col_name if isinstance(col_name, str) else str(col_name)
                            if col_part.upper() == col_name_str.upper() or col_part == col_name_str:
                                column_found = True
                                break

                if not column_found:
                    sample_cols = sorted(
                        {
                            str(k)
                            for k in self.available_columns.keys()
                            if isinstance(k, str) and k and "__" not in k and "-" not in k
                        }
                    )[:12]
                    hint = f" Available columns: {', '.join(sample_cols)}" if sample_cols else ""
                    self.errors.append(f"Unknown column: '{token}'.{hint}")

    def _parse_function_call(self, func_name: str, start_idx: int, tokens: list[str]) -> tuple:
        """Parse a function call and return (end_index, arguments_list, has_arithmetic_in_args)"""
        if start_idx + 1 >= len(tokens) or tokens[start_idx + 1] != '(':
            return (start_idx, [], False)

        # Find matching closing parenthesis
        paren_count = 0
        arg_tokens = []
        i = start_idx + 2  # Skip function name and opening paren
        has_arithmetic = False

        while i < len(tokens):
            token = tokens[i]

            if token == '(':
                paren_count += 1
                arg_tokens.append(token)
            elif token == ')':
                if paren_count == 0:
                    # Found closing paren for this function
                    break
                paren_count -= 1
                arg_tokens.append(token)
            elif token == ',' and paren_count == 0:
                # Argument separator at top level
                arg_tokens.append(token)
            else:
                arg_tokens.append(token)
                # Check for arithmetic operators
                if token in ['+', '-', '*', '/']:
                    has_arithmetic = True

            i += 1

        # Parse arguments (split by commas at top level)
        arguments = []
        current_arg = []
        paren_level = 0

        for token in arg_tokens:
            if token == '(':
                paren_level += 1
                current_arg.append(token)
            elif token == ')':
                paren_level -= 1
                current_arg.append(token)
            elif token == ',' and paren_level == 0:
                if current_arg:
                    arguments.append(' '.join(current_arg))
                    current_arg = []
            else:
                current_arg.append(token)

        if current_arg:
            arguments.append(' '.join(current_arg))

        return (i, arguments, has_arithmetic)

    def _validate_functions(self, tokens: list[str]) -> None:
        """Validate that all function names are supported and have correct signatures"""
        zero_arg_keywords = {"CURRENT_DATE", "CURRENT_TIMESTAMP"}
        i = 0
        while i < len(tokens):
            token = tokens[i]
            token_upper = token.upper()

            if token_upper in SUPPORTED_FUNCTIONS:
                func_info = SUPPORTED_FUNCTIONS[token_upper]

                # PostgreSQL treats CURRENT_DATE/CURRENT_TIMESTAMP as special keywords.
                # Accept bare usage (without parentheses) and reject "()" form with a clear message.
                if token_upper in zero_arg_keywords:
                    if i + 1 < len(tokens) and tokens[i + 1] == '(':
                        self.errors.append(f"Use '{token_upper}' without parentheses")
                    i += 1
                    continue

                # CAST has SQL-specific syntax: CAST(expr AS type)
                if token_upper == 'CAST':
                    end_idx, arguments, _ = self._parse_function_call(token, i, tokens)
                    if end_idx == i:
                        self.errors.append("Function 'CAST' must be followed by opening parenthesis")
                        i += 1
                        continue

                    # Validate "AS" appears at top level inside CAST(...)
                    cast_tokens = tokens[i + 2:end_idx]  # inside CAST(...)
                    depth = 0
                    has_top_level_as = False
                    for t in cast_tokens:
                        if t == '(':
                            depth += 1
                        elif t == ')':
                            depth -= 1
                        elif depth == 0 and str(t).upper() == 'AS':
                            has_top_level_as = True
                            break
                    if not has_top_level_as:
                        self.errors.append("CAST must use SQL syntax: CAST(expression AS type)")

                    i = end_idx + 1
                    continue

                # Parse function call
                end_idx, arguments, has_arithmetic = self._parse_function_call(token, i, tokens)

                if end_idx == i:
                    # Function not followed by opening parenthesis
                    self.errors.append(f"Function '{token}' must be followed by opening parenthesis")
                    i += 1
                    continue

                # Validate CONCAT specifically
                if token_upper == 'CONCAT':
                    if len(arguments) < 2:
                        self.errors.append(
                            "CONCAT expects at least 2 comma-separated arguments (CONCAT(a, b, ...)). "
                            f"Found {len(arguments)} argument(s)."
                        )

                    # Check if CONCAT has arithmetic operators inside (incorrect usage)
                    if has_arithmetic:
                        # Check if it's a single argument with arithmetic (like CONCAT(a + b))
                        if len(arguments) == 1:
                            self.errors.append(
                                "CONCAT expects comma-separated arguments (CONCAT(a, b, ...)). "
                                "Use commas instead of '+' for concatenation."
                            )
                        else:
                            # Multiple arguments but one has arithmetic - warn but allow if it's numeric
                            pass

                # Validate argument count for other functions
                min_args = func_info.get('min_args', 0)
                max_args = func_info.get('max_args')

                if len(arguments) < min_args:
                    self.errors.append(
                        f"Function '{token}' requires at least {min_args} argument(s), "
                        f"but found {len(arguments)}"
                    )

                if max_args is not None and len(arguments) > max_args:
                    self.errors.append(
                        f"Function '{token}' accepts at most {max_args} argument(s), "
                        f"but found {len(arguments)}"
                    )

                i = end_idx + 1
            else:
                i += 1

    def _get_column_type(self, column_name: str) -> Optional[str]:
        """Get the datatype of a column"""
        # Handle table-prefixed columns
        if '.' in column_name:
            column_name = column_name.split('.')[-1]

        for col_name, col_info in self.available_columns.items():
            col_name_str = col_name if isinstance(col_name, str) else str(col_name)
            if column_name.upper() == col_name_str.upper() or column_name == col_name_str:
                if isinstance(col_info, dict):
                    return col_info.get('datatype', 'TEXT')
                return 'TEXT'
        return None

    def _normalize_type(self, datatype: str) -> str:
        """Normalize datatype to base type"""
        datatype_upper = datatype.upper()
        if datatype_upper in ['STRING', 'TEXT', 'VARCHAR', 'CHAR']:
            return 'STRING'
        elif datatype_upper in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']:
            return 'INTEGER'
        elif datatype_upper in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL']:
            return 'DECIMAL'
        elif datatype_upper in ['BOOLEAN', 'BOOL']:
            return 'BOOLEAN'
        elif datatype_upper in ['DATE', 'TIMESTAMP', 'DATETIME', 'TIME']:
            return 'DATE'
        return 'STRING'  # Default

    def _validate_operators(self, tokens: list[str]) -> None:
        """Validate operator type compatibility"""
        i = 0
        while i < len(tokens):
            token = tokens[i]

            # Check for '+' operator (most problematic for type mismatches)
            if token == '+':
                # Find left and right operands
                left_operand = None
                right_operand = None

                # Look backwards for left operand
                j = i - 1
                while j >= 0:
                    if tokens[j] not in ['(', ')', ','] and tokens[j] not in OPERATORS:
                        left_operand = tokens[j]
                        break
                    j -= 1

                # Look forwards for right operand
                j = i + 1
                while j < len(tokens):
                    if tokens[j] not in ['(', ')', ','] and tokens[j] not in OPERATORS:
                        right_operand = tokens[j]
                        break
                    j += 1

                # Get types of operands
                if left_operand and right_operand:
                    left_type = None
                    right_type = None

                    # Check if operands are columns
                    if not (left_operand.startswith("'") and left_operand.endswith("'")):
                        if not left_operand.replace('.', '').replace('-', '').isdigit():
                            left_type = self._get_column_type(left_operand)

                    if not (right_operand.startswith("'") and right_operand.endswith("'")):
                        if not right_operand.replace('.', '').replace('-', '').isdigit():
                            right_type = self._get_column_type(right_operand)

                    # Normalize types
                    if left_type:
                        left_type = self._normalize_type(left_type)
                    if right_type:
                        right_type = self._normalize_type(right_type)

                    # Validate type compatibility for '+'
                    if left_type and right_type:
                        # '+' is only valid for:
                        # - Numeric + Numeric
                        # - Date + Interval (simplified: allow Date + anything numeric)
                        # - String + String (but should use CONCAT instead)

                        incompatible_pairs = [
                            ('BOOLEAN', 'STRING'),
                            ('STRING', 'BOOLEAN'),
                            ('BOOLEAN', 'INTEGER'),
                            ('INTEGER', 'BOOLEAN'),
                            ('BOOLEAN', 'DECIMAL'),
                            ('DECIMAL', 'BOOLEAN'),
                        ]

                        if (left_type, right_type) in incompatible_pairs:
                            self.errors.append(
                                f"Operator '+' is not supported between types {left_type} and {right_type}. "
                                "Use CONCAT() for string concatenation or ensure both operands are numeric."
                            )
                        elif left_type == 'STRING' and right_type == 'STRING':
                            # Suggest CONCAT instead
                            self.errors.append(
                                "Use CONCAT() for string concatenation instead of '+'. "
                                f"Example: CONCAT({left_operand}, {right_operand})"
                            )

            i += 1

    def _infer_type(self, tokens: list[str]) -> None:
        """Infer the data type of the expression"""
        # Check for string functions
        has_string_func = any(token.upper() in ['CONCAT', 'UPPER', 'LOWER', 'SUBSTRING', 'TRIM', 'REPLACE', 'TO_CHAR']
                             for token in tokens)
        if has_string_func:
            self.inferred_type = 'STRING'
            return

        # Check for numeric functions
        has_numeric_func = any(token.upper() in ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'ROUND', 'ABS', 'CEIL', 'FLOOR', 'TO_NUMBER']
                              for token in tokens)
        if has_numeric_func:
            self.inferred_type = 'INTEGER' if 'COUNT' in [t.upper() for t in tokens] else 'DECIMAL'
            return

        # Check for date functions
        has_date_func = any(token.upper() in ['NOW', 'CURRENT_DATE', 'CURRENT_TIMESTAMP', 'TO_DATE', 'DATE_TRUNC', 'DATE_PART', 'EXTRACT']
                           for token in tokens)
        if has_date_func:
            self.inferred_type = 'DATE'
            return

        # Check for arithmetic operators (suggests numeric, but validate types first)
        has_arithmetic = any(op in tokens for op in ['+', '-', '*', '/'])
        if has_arithmetic:
            # Type inference for arithmetic depends on operand types
            # For now, default to DECIMAL (will be validated by operator validation)
            self.inferred_type = 'DECIMAL'
            return

        # Default to STRING if we can't determine
        self.inferred_type = 'STRING'

    def _check_type_compatibility(self) -> None:
        """Check if inferred type matches expected type"""
        if not self.inferred_type or not self.expected_data_type:
            return

        # Type mapping for compatibility
        type_map = {
            'STRING': ['STRING', 'TEXT', 'VARCHAR'],
            'INTEGER': ['INTEGER', 'INT', 'BIGINT'],
            'DECIMAL': ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL'],
            'DATE': ['DATE', 'TIMESTAMP', 'DATETIME'],
            'BOOLEAN': ['BOOLEAN', 'BOOL'],
        }

        expected_upper = self.expected_data_type.upper()
        inferred_upper = self.inferred_type.upper()

        # Check if types are compatible
        compatible = False
        for base_type, variants in type_map.items():
            if inferred_upper == base_type or inferred_upper in variants:
                if expected_upper == base_type or expected_upper in variants:
                    compatible = True
                    break

        if not compatible:
            self.errors.append(
                f"Type mismatch: Expression returns {self.inferred_type}, but {self.expected_data_type} was selected"
            )

    def _build_response(self) -> dict[str, Any]:
        """Build the validation response"""
        return {
            "success": len(self.errors) == 0,
            "errors": self.errors,
            "inferred_type": self.inferred_type,
        }

class ValidateExpressionView(APIView):
    """API endpoint for validating calculated column expressions"""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _map_to_pg_type(datatype) -> str:
        dt = (datatype or "TEXT").strip().upper()
        if any(x in dt for x in ("INT", "SERIAL")):
            return "integer"
        if any(x in dt for x in ("NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "REAL")):
            return "numeric"
        if any(x in dt for x in ("BOOL",)):
            return "boolean"
        if "DATE" in dt and "TIME" not in dt:
            return "date"
        if "TIMESTAMP" in dt or "DATETIME" in dt or "TIME" in dt:
            return "timestamp"
        return "text"

    def _validate_expression_sql_syntax(
        self,
        expression: str,
        available_columns: list[dict[str, Any]],
        expression_context: str = "filter",
    ):
        """
        Server-side SQL parser validation using PostgreSQL EXPLAIN.
        Builds an in-memory typed row so expressions with column references can be parsed safely.
        Returns error text on failure, or None when valid.
        """
        if not expression or not available_columns:
            return "Expression and available columns are required for SQL validation"

        # Build one-row typed projection: SELECT NULL::text AS "col_a", NULL::integer AS "col_b", ...
        select_parts = []
        seen = set()
        for col in available_columns:
            name = (col.get("name") or col.get("business_name") or col.get("technical_name") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            pg_type = self._map_to_pg_type(col.get("datatype"))
            safe_name = name.replace('"', '""')
            select_parts.append(f'NULL::{pg_type} AS "{safe_name}"')

        if not select_parts:
            return "No valid columns available for SQL validation"

        typed_row_sql = "SELECT " + ", ".join(select_parts)
        # Use different SQL shape by context:
        # - filter: expression must be boolean (used in WHERE)
        # - calculated: expression can be scalar (selected as a column)
        if (expression_context or "").strip().lower() == "calculated":
            sql = f'EXPLAIN SELECT ({expression}) AS "__expr_result" FROM ({typed_row_sql}) __expr_cols LIMIT 0'
        else:
            sql = f"EXPLAIN SELECT 1 FROM ({typed_row_sql}) __expr_cols WHERE {expression} LIMIT 0"

        conn = None
        cur = None
        try:
            conn = psycopg2.connect(
                host=settings.DATABASES["default"]["HOST"],
                port=settings.DATABASES["default"]["PORT"],
                user=settings.DATABASES["default"]["USER"],
                password=settings.DATABASES["default"]["PASSWORD"],
                database=settings.DATABASES["default"]["NAME"],
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            return None
        except Exception as e:
            msg = str(e)
            # Common user-facing case for filter expressions:
            # WHERE expects boolean; plain numeric/text expressions are invalid here.
            if "argument of WHERE must be type boolean" in msg:
                return (
                    "Filter expression must evaluate to TRUE/FALSE. "
                    "Use a comparison, e.g. COALESCE(del_rec, 0) > 0, "
                    "end_time > CURRENT_DATE, or status = 'ACTIVE'."
                )
            return msg
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    def post(self, request):
        """
        Validate a calculated column expression

        Request body:
        {
            "expression": "CONCAT(firstname, ' ', lastname)",
            "expected_data_type": "STRING",
            "available_columns": [
                {"name": "firstname", "datatype": "TEXT"},
                {"name": "lastname", "datatype": "TEXT"}
            ],
            "allowed_functions": ["CONCAT", "UPPER", "LOWER", ...]  # Optional
        }

        Response:
        {
            "success": true/false,
            "errors": ["error1", "error2"],
            "inferred_type": "STRING"
        }
        """
        try:
            expression = request.data.get('expression', '').strip()
            expected_data_type = request.data.get('expected_data_type')
            available_columns = request.data.get('available_columns', [])
            allowed_functions = request.data.get('allowed_functions')  # Optional override

            if not expression:
                return Response(
                    {"success": False, "errors": ["Expression is required"], "inferred_type": None},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not available_columns:
                return Response(
                    {"success": False, "errors": ["Available columns are required"], "inferred_type": None},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Normalize available_columns format
            normalized_columns = []
            for col in available_columns:
                if isinstance(col, dict):
                    normalized_columns.append(col)
                elif isinstance(col, str):
                    normalized_columns.append({"name": col, "datatype": "TEXT"})
                else:
                    normalized_columns.append({"name": str(col), "datatype": "TEXT"})

            # Create validator
            validator = ExpressionValidator(
                expression=expression,
                available_columns=normalized_columns,
                expected_data_type=expected_data_type
            )

            # Override allowed functions if provided
            if allowed_functions:
                global SUPPORTED_FUNCTIONS
                original_functions = SUPPORTED_FUNCTIONS.copy()
                SUPPORTED_FUNCTIONS = {k: v for k, v in SUPPORTED_FUNCTIONS.items() if k in allowed_functions}

            # Validate (Python/static validation)
            result = validator.validate()

            # Optional server-side SQL parser validation (PostgreSQL EXPLAIN)
            # This catches syntax/type issues that client-side checks miss.
            sql_validation = request.data.get("sql_validation", True)
            if result.get("success") and sql_validation:
                # Infer context:
                # - calculated column flow sends expected_data_type
                # - filter flow does not
                expression_context = "calculated" if expected_data_type else "filter"
                sql_error = self._validate_expression_sql_syntax(
                    expression,
                    normalized_columns,
                    expression_context=expression_context,
                )
                if sql_error:
                    result["success"] = False
                    result.setdefault("errors", []).append(f"SQL validation failed: {sql_error}")
                    result["sql_valid"] = False
                else:
                    result["sql_valid"] = True

            # Restore original functions if overridden
            if allowed_functions:
                SUPPORTED_FUNCTIONS = original_functions

            logger.info(f"Expression validation: {result['success']}, errors: {result['errors']}")

            return Response(result, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error validating expression: {e}", exc_info=True)
            return Response(
                {"success": False, "errors": [f"Validation error: {e!s}"], "inferred_type": None},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
