# Moved from: hana_connection/hana.py
"""
Comprehensive HANA Service - Single file containing all functionality
"""

import json
import logging
from typing import Any, Optional

import psycopg2
import psycopg2.extras

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# HANA Connection imports
try:
    from hdbcli import dbapi as hana_dbapi
    HANA_AVAILABLE = True
except ImportError:
    HANA_AVAILABLE = False
    logger.warning("hdbcli package not available. HANA functionality will be limited.")

class HanaConnection:
    """SAP HANA database connection handler"""

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str, schema: str = "MIG_COCKPIT", table_name: str = "/1LT/DS_MAPPING"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.connection = None

        if not HANA_AVAILABLE:
            raise ImportError("hdbcli package not available. Please install it with: pip install hdbcli")

    def connect(self) -> bool:
        """Connect to HANA database"""
        try:
            # Connection parameters matching working format
            connection_params = {
                'address': self.host,
                'port': self.port,
                'user': self.user,
                'password': self.password,
                'databaseName': self.database
            }

            logger.info(f"Attempting to connect to HANA: {self.host}:{self.port}")
            logger.info(f"Connection params: address={connection_params['address']}, user={connection_params['user']}, database={connection_params['databaseName']}")

            self.connection = hana_dbapi.connect(**connection_params)
            logger.info(f"Connected to HANA database: {self.database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to HANA database: {e}")
            logger.error(f"Connection details: host={self.host}, port={self.port}, user={self.user}, database={self.database}")
            return False

    def disconnect(self):
        """Disconnect from HANA database"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from HANA database")

    def test_connection(self) -> dict[str, Any]:
        """Test HANA connection and return basic info"""
        try:
            if not self.connect():
                return {"success": False, "message": "Failed to connect to HANA database"}

            cursor = self.connection.cursor()

            # Test basic query
            cursor.execute("SELECT 1 FROM DUMMY")
            cursor.fetchone()

            # Get schema info
            cursor.execute(f"SELECT SCHEMA_NAME FROM SCHEMAS WHERE SCHEMA_NAME = '{self.schema}'")
            schemas = cursor.fetchall()

            # Get table count
            cursor.execute(f"SELECT COUNT(*) FROM TABLES WHERE SCHEMA_NAME = '{self.schema}'")
            tables_count = cursor.fetchone()[0]

            cursor.close()
            self.disconnect()

            return {
                "success": True,
                "message": "HANA connection successful",
                "schemas": [s[0] for s in schemas],
                "tables_count": tables_count
            }

        except Exception as e:
            logger.error(f"HANA connection test failed: {e}")
            return {"success": False, "message": f"HANA connection test failed: {e!s}"}

    def get_mapping_data(self) -> list[dict[str, Any]]:
        """Get unique mapping data from HANA"""
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Failed to connect to HANA database")

            cursor = self.connection.cursor()

            # Get unique combinations of SYS_ID, MT_ID, COBJ_IDENT
            query = """
            SELECT DISTINCT SYS_ID, MT_ID, COBJ_IDENT
            FROM "{self.schema}"."{self.table_name}"
            ORDER BY SYS_ID, MT_ID, COBJ_IDENT
            """

            cursor.execute(query)
            results = cursor.fetchall()

            # Convert to list of dictionaries
            mapping_data = []
            for row in results:
                mapping_data.append({
                    'SYS_ID': row[0],
                    'MT_ID': row[1],
                    'COBJ_IDENT': row[2]
                })

            cursor.close()
            logger.info(f"Retrieved {len(mapping_data)} unique mapping records from HANA")
            return mapping_data

        except Exception as e:
            logger.error(f"Failed to get mapping data from HANA: {e}")
            raise Exception(f"Failed to get mapping data from HANA: {e!s}")

    def get_staging_tables_for_project(self, sys_id: str, mt_id: str) -> list[dict[str, Any]]:
        """Get staging tables for specific project"""
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Failed to connect to HANA database")

            cursor = self.connection.cursor()

            # Get staging tables for specific SYS_ID and MT_ID
            query = """
            SELECT SYS_ID, MT_ID, COBJ_IDENT, STRUCT_IDENT, STAGING_TAB
            FROM "{self.schema}"."{self.table_name}"
            WHERE SYS_ID = ? AND MT_ID = ?
            ORDER BY COBJ_IDENT, STRUCT_IDENT
            """

            cursor.execute(query, (sys_id, mt_id))
            results = cursor.fetchall()

            # Convert to list of dictionaries
            staging_data = []
            for row in results:
                staging_data.append({
                    'SYS_ID': row[0],
                    'MT_ID': row[1],
                    'COBJ_IDENT': row[2],
                    'STRUCT_IDENT': row[3],
                    'STAGING_TAB': row[4]
                })

            cursor.close()
            logger.info(f"Retrieved {len(staging_data)} staging tables for project {sys_id}_{mt_id}")
            return staging_data

        except Exception as e:
            logger.error(f"Failed to get staging tables for project {sys_id}_{mt_id}: {e}")
            raise Exception(f"Failed to get staging tables for project {sys_id}_{mt_id}: {e!s}")

    def get_project_data(self) -> list[dict[str, Any]]:
        """Get project data from HANA using complex join query"""
        try:
            if not self.connection:
                if not self.connect():
                    raise Exception("Failed to connect to HANA database")

            cursor = self.connection.cursor()

            # New complex query joining multiple tables
            query = """
            SELECT HDR.ID, OBJ.SOURCE, OBJ.IDENT, HDR.DESCR, HDR.SUBPROJECT, OBJ.COBJ_ALIAS, HDR.IS_ACTIVE,
                   PRCT.REF_CLIENT, HDR.ACTIVE_PHASE, OBJ.CONTENT_DATE, OBJ.CONTENT_TIME, OBJ.AUTHOR, OBJ.CREATEDATE,
                   OBJ.GUID AS OBJ_GUID, SPRJ.GUID AS SPRJ_GUID
            FROM "SAPHANADB"."/LTB/MC_PROJ" MPRJ
            INNER JOIN "SAPHANADB"."DMC_PRJCT" PRCT
                ON MPRJ.UUID = PRCT.GUID AND MPRJ.APPROACH = 'STAGING'
            LEFT JOIN "SAPHANADB"."DMC_SPRJCT" AS SPRJ
                ON MPRJ.UUID = SPRJ.PROJECT
            LEFT JOIN "SAPHANADB"."DMC_COBJ" OBJ
                ON OBJ.SUBPROJECT = SPRJ.GUID
            INNER JOIN "SAPHANADB"."DMC_MT_HEADER" AS HDR
                ON SPRJ.IDENT = HDR.SUBPROJECT
            WHERE MPRJ.TO_BE_DELETED <> 'X'
            """

            logger.info(f"Executing testdata query: {query}")
            cursor.execute(query)
            results = cursor.fetchall()

            # Convert to list of dictionaries
            testdata = []
            for row in results:
                testdata.append({
                    'ID': row[0],
                    'SOURCE': row[1],
                    'IDENT': row[2],
                    'DESCR': row[3],
                    'SUBPROJECT': row[4],
                    'COBJ_ALIAS': row[5],
                    'IS_ACTIVE': row[6],
                    'REF_CLIENT': row[7],
                    'ACTIVE_PHASE': row[8],
                    'CONTENT_DATE': row[9],
                    'CONTENT_TIME': row[10],
                    'AUTHOR': row[11],
                    'CREATEDATE': row[12],
                    'OBJ_GUID': row[13],
                    'SPRJ_GUID': row[14]
                })

            cursor.close()
            logger.info(f"Retrieved {len(testdata)} project records from HANA")
            return testdata

        except Exception as e:
            logger.error(f"Failed to get project data from HANA: {e}")
            raise Exception(f"Failed to get project data from HANA: {e!s}")

class DatabaseManager:
    """PostgreSQL database manager with safe creation"""

    def __init__(self, config_file: str = "postgres_config.json", customer_db: str = "C00001"):
        with open(config_file) as f:
            config = json.load(f)

        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self.customer_db = customer_db
        self.connection = None

    def connect(self):
        """Connect to main MIGDATA database"""
        try:
            logger.info(f"🔌 Attempting to connect to PostgreSQL: {self.host}:{self.port}")
            logger.info(f"📊 Database: {self.database}, User: {self.user}")

            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            self.connection.autocommit = True
            logger.info("✅ Successfully connected to PostgreSQL MIGDATA database")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            return False

    def ensure_customer_database_exists(self):
        """Ensure customer database exists with safe creation"""
        # Connect to PostgreSQL server (not specific database)
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database='postgres'  # Connect to default database
        )
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Create database if not exists (PostgreSQL doesn't have CREATE DATABASE IF NOT EXISTS)
            cursor.execute(f'CREATE DATABASE "{self.customer_db}"')
            logger.info(f"Created customer database: {self.customer_db}")

            # Create GENERAL schema and PROJECT table
            self._create_general_schema()

        except psycopg2.errors.DuplicateDatabase:
            logger.info(f"Customer database {self.customer_db} already exists")
            # Still ensure GENERAL schema exists
            self._create_general_schema()

        cursor.close()
        conn.close()

    def _create_general_schema(self):
        """Create GENERAL schema with PROJECT table"""
        # Connect to customer database
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.customer_db
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create GENERAL schema
        cursor.execute('CREATE SCHEMA IF NOT EXISTS "GENERAL"')

        # Create PROJECT table with all CAPITAL field names
        project_table = """
        CREATE TABLE IF NOT EXISTS "GENERAL"."PROJECT" (
            "ID" VARCHAR(50) NOT NULL,
            "SOURCE" VARCHAR(50),
            "IDENT" VARCHAR(50),
            "DESCR" VARCHAR(255),
            "SUBPROJECT" VARCHAR(100),
            "COBJ_ALIAS" VARCHAR(100),
            "IS_ACTIVE" BOOLEAN,
            "REF_CLIENT" VARCHAR(50),
            "ACTIVE_PHASE" VARCHAR(50),
            "CONTENT_DATE" DATE,
            "CONTENT_TIME" TIME,
            "AUTHOR" VARCHAR(100),
            "CREATEDATE" TIMESTAMP,
            "OBJ_GUID" VARCHAR(100),
            "SPRJ_GUID" VARCHAR(100),
            "CREATED_ON" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "MODIFIED_ON" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT "PROJECT_PKEY" PRIMARY KEY ("ID")
        )
        """
        cursor.execute(project_table)

        logger.info("Created GENERAL schema and PROJECT table")

        cursor.close()
        conn.close()

    def create_project_schema_and_table(self, sys_id: str, mt_id: str):
        """Create project schema (SYS_ID + MT_ID) with STAGE_TBLE table"""
        schema_name = f"{sys_id}_{mt_id}"

        # Connect to customer database
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.customer_db
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create project schema
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')

        # Create STAGE_TBLE table with all CAPITAL field names
        stage_table = """
        CREATE TABLE IF NOT EXISTS "{schema_name}"."STAGE_TBLE" (
            "SYS_ID" VARCHAR(3) NOT NULL,
            "MT_ID" VARCHAR(3) NOT NULL,
            "COBJ_IDENT" VARCHAR(20) NOT NULL,
            "STRUCT_IDENT" VARCHAR(20) NOT NULL,
            "STAGING_TAB" VARCHAR(30) NOT NULL,
            "CREATED_ON" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "MODIFIED_ON" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT "STAGE_TBLE_PKEY" PRIMARY KEY ("SYS_ID", "MT_ID", "COBJ_IDENT")
        )
        """
        cursor.execute(stage_table)
        logger.info(f"Created schema {schema_name} with STAGE_TBLE table")

        cursor.close()
        conn.close()

    def insert_staging_table(self, sys_id: str, mt_id: str, cobj_ident: str, struct_ident: str, staging_tab: str) -> Optional[dict[str, Any]]:
        """Insert staging table data into project schema"""

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.customer_db
        )
        conn.autocommit = True
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
        INSERT INTO "{schema_name}"."STAGE_TBLE"
        ("SYS_ID", "MT_ID", "COBJ_IDENT", "STRUCT_IDENT", "STAGING_TAB")
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ("SYS_ID", "MT_ID", "COBJ_IDENT") DO NOTHING
        RETURNING *
        """

        cursor.execute(query, (sys_id, mt_id, cobj_ident, struct_ident, staging_tab))
        result = cursor.fetchone()

        cursor.close()
        conn.close()
        return result

    def insert_project(self, id_val: str, source: str, ident: str, descr: str, subproject: str,
                       cobj_alias: str, is_active: bool, ref_client: str, active_phase: str, content_date: str,
                       content_time: str, author: str, createdate: str, obj_guid: str, sprj_guid: str) -> Optional[dict[str, Any]]:
        """Insert project record into GENERAL.PROJECT table"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.customer_db
            )
            conn.autocommit = True
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Convert None values to None for proper NULL handling
            def safe_str(val):
                return str(val) if val is not None else None

            def safe_bool(val):
                return bool(val) if val is not None else None

            def safe_date(val):
                if val is None:
                    return None
                try:
                    # Handle different date formats from HANA
                    if isinstance(val, str):
                        from datetime import datetime
                        return datetime.strptime(val.split(' ')[0], '%Y-%m-%d').date()
                    return val
                except Exception:
                    return None

            def safe_time(val):
                if val is None:
                    return None
                try:
                    # Handle different time formats from HANA
                    if isinstance(val, str):
                        from datetime import datetime
                        return datetime.strptime(val.split(' ')[-1], '%H:%M:%S').time()
                    return val
                except Exception:
                    return None

            def safe_timestamp(val):
                if val is None:
                    return None
                try:
                    # Handle different timestamp formats from HANA
                    if isinstance(val, str):
                        from datetime import datetime
                        return datetime.fromisoformat(val.replace('Z', '+00:00'))
                    return val
                except Exception:
                    return None

            # Prepare values with proper type conversion
            values = (
                safe_str(id_val),
                safe_str(source),
                safe_str(ident),
                safe_str(descr),
                safe_str(subproject),
                safe_str(cobj_alias),
                safe_bool(is_active),
                safe_str(ref_client),
                safe_str(active_phase),
                safe_date(content_date),
                safe_time(content_time),
                safe_str(author),
                safe_timestamp(createdate),
                safe_str(obj_guid),
                safe_str(sprj_guid)
            )

            query = """
            INSERT INTO "GENERAL"."PROJECT"
            ("ID", "SOURCE", "IDENT", "DESCR", "SUBPROJECT", "COBJ_ALIAS", "IS_ACTIVE",
             "REF_CLIENT", "ACTIVE_PHASE", "CONTENT_DATE", "CONTENT_TIME", "AUTHOR", "CREATEDATE",
             "OBJ_GUID", "SPRJ_GUID")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("ID") DO UPDATE SET
                "SOURCE" = EXCLUDED."SOURCE",
                "IDENT" = EXCLUDED."IDENT",
                "DESCR" = EXCLUDED."DESCR",
                "SUBPROJECT" = EXCLUDED."SUBPROJECT",
                "COBJ_ALIAS" = EXCLUDED."COBJ_ALIAS",
                "IS_ACTIVE" = EXCLUDED."IS_ACTIVE",
                "REF_CLIENT" = EXCLUDED."REF_CLIENT",
                "ACTIVE_PHASE" = EXCLUDED."ACTIVE_PHASE",
                "CONTENT_DATE" = EXCLUDED."CONTENT_DATE",
                "CONTENT_TIME" = EXCLUDED."CONTENT_TIME",
                "AUTHOR" = EXCLUDED."AUTHOR",
                "CREATEDATE" = EXCLUDED."CREATEDATE",
                "OBJ_GUID" = EXCLUDED."OBJ_GUID",
                "SPRJ_GUID" = EXCLUDED."SPRJ_GUID",
                "MODIFIED_ON" = CURRENT_TIMESTAMP
            RETURNING *
            """

            logger.info(f"Inserting project record: ID={id_val}, SOURCE={source}")
            cursor.execute(query, values)
            result = cursor.fetchone()

            cursor.close()
            conn.close()
            return result

        except Exception as e:
            logger.error(f"Failed to insert project record {id_val}: {e}")
            logger.error(f"Values: ID={id_val}, SOURCE={source}, IDENT={ident}")
            raise e

    def get_staging_tables(self, sys_id: str, mt_id: str) -> list[dict[str, Any]]:
        """Get staging tables for specific project"""
        schema_name = f"{sys_id}_{mt_id}"

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.customer_db
        )
        conn.autocommit = True
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute(f'SELECT * FROM "{schema_name}"."STAGE_TBLE" ORDER BY "COBJ_IDENT"')
        results = cursor.fetchall()

        cursor.close()
        conn.close()
        return results

class HanaService:
    """Comprehensive HANA service combining all functionality"""

    def __init__(self, config_file: str = "postgres_config.json", customer_db: str = "C00001"):
        self.db = DatabaseManager(config_file, customer_db)
        self.hana_conn = None

    def test_postgres_connection(self) -> dict[str, Any]:
        """Test PostgreSQL connection"""
        try:
            logger.info("🧪 Testing PostgreSQL connection...")

            # Test main MIGDATA database connection
            if not self.db.connect():
                return {"success": False, "message": "Failed to connect to PostgreSQL MIGDATA database"}

            # Test basic query
            cursor = self.db.connection.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()

            # Test customer database creation
            self.db.ensure_customer_database_exists()

            logger.info("✅ PostgreSQL connection test successful")
            return {
                "success": True,
                "message": "PostgreSQL connection successful",
                "version": version,
                "main_database": self.db.database,
                "customer_database": self.db.customer_db
            }

        except Exception as e:
            logger.error(f"❌ PostgreSQL connection test failed: {e}")
            return {"success": False, "message": f"PostgreSQL connection test failed: {e!s}"}

    def test_hana_connection(self, hana_config: dict[str, Any]) -> dict[str, Any]:
        """Test HANA connection"""
        try:
            hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )
            return hana_conn.test_connection()
        except Exception as e:
            logger.error(f"HANA connection test failed: {e}")
            return {"success": False, "message": f"HANA connection test failed: {e!s}"}

    def test_project_query(self, hana_config: dict[str, Any]) -> dict[str, Any]:
        """Test the project data query specifically"""
        try:
            logger.info("🧪 Testing project data query...")

            hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )

            if not hana_conn.connect():
                return {"success": False, "message": "Failed to connect to HANA database"}

            # Test the project data query
            project_data = hana_conn.get_project_data()
            hana_conn.disconnect()

            return {
                "success": True,
                "message": f"Project data query successful, found {len(project_data)} records",
                "record_count": len(project_data),
                "sample_record": project_data[0] if project_data else None
            }

        except Exception as e:
            logger.error(f"Project data query test failed: {e}")
            return {"success": False, "message": f"Project data query test failed: {e!s}"}

    def test_both_connections(self, hana_config: dict[str, Any]) -> dict[str, Any]:
        """Test both HANA and PostgreSQL connections"""
        logger.info("🧪 Testing both HANA and PostgreSQL connections...")

        # Test PostgreSQL
        postgres_result = self.test_postgres_connection()

        # Test HANA
        hana_result = self.test_hana_connection(hana_config)

        # Overall result
        both_success = postgres_result["success"] and hana_result["success"]

        return {
            "success": both_success,
            "message": "Both connections successful" if both_success else "One or both connections failed",
            "postgres": postgres_result,
            "hana": hana_result,
            "summary": {
                "postgres_ok": postgres_result["success"],
                "hana_ok": hana_result["success"],
                "both_ok": both_success
            }
        }

    def import_projects_from_hana(self, hana_config: dict[str, Any], hana_conn: Optional[HanaConnection] = None) -> list[dict[str, Any]]:
        """Import projects from HANA and create project schemas"""
        # Use provided connection or create new one
        if hana_conn:
            self.hana_conn = hana_conn
        elif not self.hana_conn:
            self.hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )
            if not self.hana_conn.connect():
                raise Exception("Failed to connect to HANA database")

        # Ensure customer database exists
        self.db.ensure_customer_database_exists()

        # Get unique projects from HANA
        mapping_data = self.hana_conn.get_mapping_data()

        if not mapping_data:
            raise Exception("No data found in HANA MIG_COCKPIT.'/1LT/DS_MAPPING'")

        # Process each project - create schemas only
        projects = []
        for row in mapping_data:
            sys_id = row['SYS_ID']
            mt_id = row['MT_ID']
            cobj_ident = row['COBJ_IDENT']

            # Create project schema and STAGE_TBLE table
            self.db.create_project_schema_and_table(sys_id, mt_id)

            # Add to projects list for return
            projects.append({
                'SYS_ID': sys_id,
                'MT_ID': mt_id,
                'COBJ_IDENT': cobj_ident
            })

        logger.info(f"Created {len(projects)} project schemas from HANA")
        return projects

    def import_project_data_from_hana(self, hana_config: dict[str, Any], hana_conn: Optional[HanaConnection] = None) -> list[dict[str, Any]]:
        """Import project data from HANA using complex join query"""
        # Use provided connection or create new one
        if hana_conn:
            self.hana_conn = hana_conn
        elif not self.hana_conn:
            self.hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )
            if not self.hana_conn.connect():
                raise Exception("Failed to connect to HANA database")

        # Get project data from HANA
        project_data = self.hana_conn.get_project_data()

        if not project_data:
            logger.warning("No project data found in HANA")
            return []

        # Insert project data records
        project_records = []
        failed_records = []

        logger.info(f"Starting to insert {len(project_data)} project records...")

        for i, row in enumerate(project_data):
            try:
                logger.info(f"Processing project record {i+1}/{len(project_data)}: ID={row.get('ID', 'N/A')}")

                result = self.db.insert_project(
                    id_val=row.get('ID'),
                    source=row.get('SOURCE'),
                    ident=row.get('IDENT'),
                    descr=row.get('DESCR'),
                    subproject=row.get('SUBPROJECT'),
                    cobj_alias=row.get('COBJ_ALIAS'),
                    is_active=row.get('IS_ACTIVE'),
                    ref_client=row.get('REF_CLIENT'),
                    active_phase=row.get('ACTIVE_PHASE'),
                    content_date=row.get('CONTENT_DATE'),
                    content_time=row.get('CONTENT_TIME'),
                    author=row.get('AUTHOR'),
                    createdate=row.get('CREATEDATE'),
                    obj_guid=row.get('OBJ_GUID'),
                    sprj_guid=row.get('SPRJ_GUID')
                )
                if result:
                    project_records.append(result)
                    logger.info(f"✅ Successfully inserted project record: ID={row.get('ID')}")
                else:
                    logger.warning(f"⚠️ No result returned for project record: ID={row.get('ID')}")
                    failed_records.append(row.get('ID', f'Record_{i+1}'))
            except Exception as e:
                logger.error(f"❌ Failed to insert project record {row.get('ID', f'Record_{i+1}')}: {e}")
                failed_records.append(row.get('ID', f'Record_{i+1}'))

        logger.info(f"Imported {len(project_records)} project records from HANA")
        if failed_records:
            logger.warning(f"Failed to import {len(failed_records)} project records: {failed_records}")

        return project_records

    def import_staging_tables_from_hana(self, sys_id: str, mt_id: str, hana_config: dict[str, Any], hana_conn: Optional[HanaConnection] = None) -> list[dict[str, Any]]:
        """Import staging tables for specific project from HANA"""
        # Use provided connection or create new one
        if hana_conn:
            self.hana_conn = hana_conn
        elif not self.hana_conn:
            self.hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )
            if not self.hana_conn.connect():
                raise Exception("Failed to connect to HANA database")

        # Get staging tables for specific project
        staging_data = self.hana_conn.get_staging_tables_for_project(sys_id, mt_id)

        if not staging_data:
            logger.warning(f"No staging tables found for project {sys_id}_{mt_id}")
            return []

        # Insert staging table data
        staging_tables = []
        for row in staging_data:
            result = self.db.insert_staging_table(
                row['SYS_ID'], row['MT_ID'], row['COBJ_IDENT'],
                row['STRUCT_IDENT'], row['STAGING_TAB']
            )
            if result:
                staging_tables.append(result)

        logger.info(f"Imported {len(staging_tables)} staging tables for project {sys_id}_{mt_id}")
        return staging_tables

    def import_testdata_from_hana(self, hana_config: dict[str, Any], hana_conn: Optional[HanaConnection] = None) -> list[dict[str, Any]]:
        """Import testdata from HANA DMC_MT_HEADER and DMC_COBJ tables"""
        # Use provided connection or create new one
        if hana_conn:
            self.hana_conn = hana_conn
        elif not self.hana_conn:
            self.hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )
            if not self.hana_conn.connect():
                raise Exception("Failed to connect to HANA database")

        # Get testdata from HANA
        testdata = self.hana_conn.get_testdata()

        if not testdata:
            logger.warning("No testdata found in HANA DMC_MT_HEADER and DMC_COBJ tables")
            return []

        # Insert testdata records
        testdata_records = []
        failed_records = []

        logger.info(f"Starting to insert {len(testdata)} testdata records...")

        for i, row in enumerate(testdata):
            try:
                logger.info(f"Processing testdata record {i+1}/{len(testdata)}: ID={row.get('ID', 'N/A')}")

                result = self.db.insert_testdata(
                    id_val=row.get('ID'),
                    source=row.get('SOURCE'),
                    ident=row.get('IDENT'),
                    descr=row.get('DESCR'),
                    subproject=row.get('SUBPROJECT'),
                    cobj_alias=row.get('COBJ_ALIAS'),
                    is_active=row.get('IS_ACTIVE'),
                    ref_client=row.get('REF_CLIENT'),
                    active_phase=row.get('ACTIVE_PHASE'),
                    content_date=row.get('CONTENT_DATE'),
                    content_time=row.get('CONTENT_TIME'),
                    author=row.get('AUTHOR'),
                    createdate=row.get('CREATEDATE'),
                    obj_guid=row.get('OBJ_GUID'),
                    sprj_guid=row.get('SPRJ_GUID')
                )
                if result:
                    testdata_records.append(result)
                    logger.info(f"✅ Successfully inserted testdata record: ID={row.get('ID')}")
                else:
                    logger.warning(f"⚠️ No result returned for testdata record: ID={row.get('ID')}")
                    failed_records.append(row.get('ID', f'Record_{i+1}'))
            except Exception as e:
                logger.error(f"❌ Failed to insert testdata record {row.get('ID', f'Record_{i+1}')}: {e}")
                failed_records.append(row.get('ID', f'Record_{i+1}'))

        logger.info(f"Imported {len(testdata_records)} testdata records from HANA")
        if failed_records:
            logger.warning(f"Failed to import {len(failed_records)} testdata records: {failed_records}")

        return testdata_records

    def complete_import_process(self, hana_config: dict[str, Any]) -> dict[str, Any]:
        """Complete import process - all steps in one operation"""
        try:
            logger.info("Starting complete import process...")

            # Step 1: Establish HANA connection
            logger.info("Step 1: Establishing HANA connection...")
            hana_conn = HanaConnection(
                host=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['user'],
                password=hana_config['password'],
                database=hana_config['database'],
                schema=hana_config.get('schema', 'MIG_COCKPIT'),
                table_name=hana_config.get('table_name', '/1LT/DS_MAPPING')
            )

            if not hana_conn.connect():
                raise Exception("Failed to connect to HANA database")

            logger.info("✅ HANA connection established")

            # Step 2: Import projects from HANA
            logger.info("Step 2: Importing projects from HANA...")
            projects = self.import_projects_from_hana(hana_config, hana_conn)
            logger.info(f"✅ Imported {len(projects)} projects")

            # Step 2.5: Import project data from HANA
            logger.info("Step 2.5: Importing project data from HANA...")
            project_records = self.import_project_data_from_hana(hana_config, hana_conn)
            logger.info(f"✅ Imported {len(project_records)} project records")

            # Step 3: Import staging tables for all project schemas
            logger.info("Step 3: Importing staging tables for all project schemas...")
            staging_results = []

            for project in projects:
                try:
                    staging_tables = self.import_staging_tables_from_hana(
                        project['SYS_ID'],
                        project['MT_ID'],
                        hana_config,
                        hana_conn
                    )
                    staging_results.append({
                        "project": f"{project['SYS_ID']}_{project['MT_ID']}",
                        "staging_tables_count": len(staging_tables),
                        "status": "success"
                    })
                    logger.info(f"✅ Imported {len(staging_tables)} staging tables for {project['SYS_ID']}_{project['MT_ID']}")
                except Exception as e:
                    staging_results.append({
                        "project": f"{project['SYS_ID']}_{project['MT_ID']}",
                        "staging_tables_count": 0,
                        "status": "failed",
                        "error": str(e)
                    })
                    logger.error(f"❌ Failed to import staging tables for {project['SYS_ID']}_{project['MT_ID']}: {e}")

            # Step 4: Disconnect from HANA
            hana_conn.disconnect()
            logger.info("✅ HANA connection closed")

            # Step 5: Return comprehensive results
            result = {
                "success": True,
                "message": "Complete import process finished successfully",
                "hana_connection": "success",
                "project_schemas_created": len(projects),
                "project_records_imported": len(project_records),
                "staging_tables_results": staging_results,
                "total_staging_tables": sum(r["staging_tables_count"] for r in staging_results),
                "summary": {
                    "total_project_schemas": len(projects),
                    "total_project_records": len(project_records),
                    "successful_staging_imports": len([r for r in staging_results if r["status"] == "success"]),
                    "failed_staging_imports": len([r for r in staging_results if r["status"] == "failed"])
                }
            }

            logger.info("🎉 Complete import process finished successfully")
            return result

        except Exception as e:
            logger.error(f"Error in complete import process: {e}")
            raise Exception(f"Complete import process failed: {e!s}")

    def get_staging_tables(self, sys_id: str, mt_id: str) -> list[dict[str, Any]]:
        """Get staging tables for specific project"""
        return self.db.get_staging_tables(sys_id, mt_id)

# Default HANA configuration
DEFAULT_HANA_CONFIG = {
    "host": "10.10.100.161",
    "port": 31015,
    "user": "BODS_USR",
    "password": "DataSyncher6",
    "database": "FAD",
    "schema": "MIG_COCKPIT",
    "table_name": "/1LT/DS_MAPPING"
}

# Convenience function to create HANA connection
def create_hana_connection(config: Optional[dict[str, Any]] = None) -> HanaConnection:
    """Create HANA connection with default or custom config"""
    if config is None:
        config = DEFAULT_HANA_CONFIG

    return HanaConnection(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        schema=config.get('schema', 'MIG_COCKPIT'),
        table_name=config.get('table_name', '/1LT/DS_MAPPING')
    )
