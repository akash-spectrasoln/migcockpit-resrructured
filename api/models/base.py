"""
Base models for the API application.
Contains core application models: User, Customer, Country, SourceDB, SourceForm, Roles, UsrRoles, ValidationRules,
and connection-related models: SourceModel, SourceAttribute, SourceConfig, DestinationModel, DestinationAttribute, DestinationConfig.
"""
from django.contrib.auth.models import AbstractUser, BaseUserManager
from django.db import models
from django.utils import timezone


class SourceDB(models.Model):
    src_db = models.CharField(max_length=255)

    def __str__(self):
        return self.src_db

    class Meta:
        db_table = 'sourcedb'

class SourceForm(models.Model):
    src_db = models.ForeignKey(SourceDB, on_delete=models.CASCADE)
    attribute_name = models.CharField(max_length=100)
    input_type = models.CharField(max_length=100)
    label_name = models.CharField(max_length=100)
    is_required = models.BooleanField(default=False)

    class Meta:
        db_table = 'sourceform'

class Country(models.Model):
    """Model representing countries."""

    country_id = models.CharField(max_length=100, verbose_name='Country ID')
    name = models.CharField(max_length=100, verbose_name='Country Name')

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'country'

class UserManager(BaseUserManager):
    """Custom user manager for email-based authentication without username."""

    def create_user(self, email, first_name, last_name, created_by, password=None, **extra_fields):
        """Create and save a regular user with the given email and password."""
        if not email:
            raise ValueError('The Email field must be set')

        email = self.normalize_email(email)
        user = self.model(
            email=email,
            first_name=first_name,
            last_name=last_name,
            created_by=created_by,
            **extra_fields
        )
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, email, first_name, last_name, created_by, password=None, **extra_fields):
        """Create and save a superuser with the given email and password."""
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_active', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError('Superuser must have is_staff=True.')
        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True.')

        return self.create_user(email, first_name, last_name, created_by, password, **extra_fields)

class User(AbstractUser):
    """Custom User model using email as primary key."""

    # Remove the default integer PK from AbstractUser
    id = None
    # Remove username and use email instead
    username = None

    cust_id = models.ForeignKey(
        'Customer',
        on_delete=models.CASCADE,
        verbose_name='Customer ID',
        null=True,
        blank=True,
    )
    # Email is the primary key (and implicitly unique)
    email = models.EmailField(
        max_length=50,
        primary_key=True,
        verbose_name='Email',
    )
    first_name = models.CharField(max_length=50, verbose_name='First Name')
    last_name = models.CharField(max_length=50, verbose_name='Last Name')
    created_by = models.CharField(max_length=50, verbose_name='Created By')
    created_on = models.DateTimeField(default=timezone.now, verbose_name='Created On')
    modified_on = models.DateTimeField(auto_now=True, verbose_name='Modified On')
    modified_by = models.CharField(max_length=50, blank=True, null=True, verbose_name='Modified By')
    is_active = models.BooleanField(default=True, verbose_name='active',)
    download_format_choice = [
        ('csv', 'CSV'),
        ('excel', 'Excel'),
        ('tsv', 'TSV')
    ]
    file_format = models.CharField(max_length=10, choices=download_format_choice, default='csv')
    date_format = models.CharField(max_length=10, default='MM-DD-YYYY')
    objects = UserManager()  # Use custom manager for email-based authentication

    USERNAME_FIELD = 'email' # Set email as the field used for authentication
    REQUIRED_FIELDS = ['first_name', 'last_name', 'created_by']

    def __str__(self):
        return f"{self.first_name} {self.last_name}"

    def set_password(self, raw_password):
        """
        Override set_password to encrypt password using AES encryption.
        Uses user's created_on timestamp as encryption key.
        """
        import hashlib
        import json

        from api.connections.encryption import encrypt_field

        if raw_password is None:
            self.set_unusable_password()
            return

        from django.utils import timezone

        # Fetch the current time
        created_on = timezone.now()

        # Update the User model's created_on to this value
        self.created_on = created_on

        key_string = f"{created_on.strftime('%Y%m%d%H%M%S')}"
        hash_object = hashlib.sha256(key_string.encode())
        encryption_key = int(hash_object.hexdigest()[:8], 16)

        # Encrypt the password
        encrypted_password = encrypt_field(raw_password, encryption_key)

        # Store as JSON string in password field
        self.password = json.dumps(encrypted_password)

    def check_password(self, raw_password):
        """
        Override check_password to decrypt and compare passwords.
        Uses users's created_on timestamp for decryption.
        """
        import hashlib
        import json

        from api.connections.encryption import decrypt_field

        if self.password.startswith('!'):
            return False

        try:
            key_string = f"{self.created_on.strftime('%Y%m%d%H%M%S')}"
            hash_object = hashlib.sha256(key_string.encode())
            encryption_key = int(hash_object.hexdigest()[:8], 16)

            # Load encrypted password from JSON
            encrypted_data = json.loads(self.password)

            # Decrypt the password
            decrypted_password = decrypt_field(
                encrypted_data=encrypted_data[0],
                cmp_id=encryption_key,
                nonce=encrypted_data[1],
                tag=encrypted_data[2],
                salt=encrypted_data[3],
                original_type=encrypted_data[4],
                iterations=encrypted_data[5]
            )
            return decrypted_password == raw_password
        except Exception as e:
            print(f"Error checking password: {e!s}")
            return False

    class Meta:
        db_table = 'user'

class Customer(models.Model):
    """Model representing customers."""

    cust_id = models.CharField(max_length=10, verbose_name='Customer ID')
    name = models.CharField(max_length=100, verbose_name='Customer Name')
    street1 = models.CharField(max_length=100, verbose_name='Street 1')
    street2 = models.CharField(max_length=100, blank=True, null=True, verbose_name='Street 2')
    city = models.CharField(max_length=100, verbose_name='City')
    region = models.CharField(max_length=100, verbose_name='Region')
    country = models.ForeignKey(Country, on_delete=models.PROTECT, verbose_name='Country')
    phone = models.CharField(max_length=20, verbose_name='Phone')
    cust_db = models.CharField(max_length=10, unique=True, verbose_name='Customer Database')
    created_by = models.CharField(max_length=50, verbose_name='Created By')
    created_on = models.DateTimeField(default=timezone.now, verbose_name='Created On')
    modified_on = models.DateTimeField(auto_now=True, verbose_name='Modified On')
    modified_by = models.CharField(max_length=50, blank=True, null=True, verbose_name='Modified By')
    active = models.BooleanField(default=True, verbose_name='Active')

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        """Override save method to handle ID generation and tenant provisioning."""
        is_new = not self.pk
        if not self.cust_id:
            next_id = self.get_next_customer_id()
            self.cust_id = f"C{next_id:05d}"
            self.cust_db = self.cust_id
        super().save(*args, **kwargs)
        if is_new:
            from api.connections.tenant_provisioning import TenantProvisioningService
            TenantProvisioningService().provision(self)

    def get_next_customer_id(self):
        """Get the next available customer ID."""
        try:
            # Get all existing cust_ids and extract the numeric part
            existing_customers = Customer.objects.values_list('cust_id', flat=True)

            if not existing_customers:
                return 1

            # Extract numeric parts from existing cust_ids (e.g., "C00001" -> 1)
            numeric_ids = []
            for cust_id in existing_customers:
                if cust_id and cust_id.startswith('C'):
                    try:
                        numeric_part = int(cust_id[1:])  # Remove 'C' prefix and convert to int
                        numeric_ids.append(numeric_part)
                    except ValueError:
                        continue

            # If no valid numeric IDs found, start with 1
            if not numeric_ids:
                return 1

            # Return the highest numeric ID + 1
            return max(numeric_ids) + 1

        except Exception as e:
            # Fallback to 1 if there's an error
            print(f"Error getting next customer ID: {e!s}")
            return 1

    # DEPRECATED: Logic moved to TenantProvisioningService. Will be removed.
    def create_customer_database(self):
        """Create a new database for the customer."""
        from django.conf import settings
        import psycopg2

        conn = None
        cursor = None

        try:
            # Connect to PostgreSQL server using the 'postgres' database to create new databases
            # Use a completely separate connection to avoid transaction conflicts
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database='postgres'  # Connect to default postgres database to create new databases
            )
            conn.autocommit = True  # Enable autocommit to avoid transaction issues
            cursor = conn.cursor()

            # Check if database already exists
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s;",
                (self.cust_db,)
            )

            if cursor.fetchone():
                print(f"Database {self.cust_db} already exists")
            else:
                # Create the database
                cursor.execute(f'CREATE DATABASE "{self.cust_db}";')
                print(f"Created database: {self.cust_db}")

                try:
                    # Create schemas in the new database
                    self.create_customer_schemas()

                except Exception as schema_error:
                    # If schema creation fails, drop the database to maintain consistency
                    print(f"Schema creation failed, rolling back database creation for {self.cust_db}")
                    cursor.execute(f'DROP DATABASE IF EXISTS "{self.cust_db}";')
                    raise schema_error

        except Exception as e:
            print(f"Error creating database {self.cust_db}: {e!s}")
            raise Exception(f"Failed to create customer database: {e!s}")

        finally:
            # Ensure connections are always closed
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # DEPRECATED: Logic moved to TenantProvisioningService. Will be removed.
    def create_customer_schemas(self):
        """Create schemas in the customer's database."""
        from django.conf import settings
        import psycopg2

        conn = None
        cursor = None

        try:
            # Connect to the customer's new database
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database=self.cust_db
            )
            conn.autocommit = True  # Enable autocommit to avoid transaction issues
            cursor = conn.cursor()

            # Create main schema for customer data
            main_schema = "GENERAL"
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{main_schema}";')

            # Create Source table in the GENERAL schema
            # Use source_name and source_config to match API expectations
            # Include project_id for project-based organization
            source_table_sql = '''
            CREATE TABLE IF NOT EXISTS "{main_schema}".source (
                id SERIAL PRIMARY KEY,
                source_name VARCHAR(255),
                source_config TEXT,
                project_id INTEGER,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );
            '''
            cursor.execute(source_table_sql)

            # If table exists with old column names, rename them
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{main_schema}' AND table_name = 'source'
            """)
            existing_columns = [row[0] for row in cursor.fetchall()]

            if 'src_name' in existing_columns and 'source_name' not in existing_columns:
                cursor.execute(f'ALTER TABLE "{main_schema}".source RENAME COLUMN src_name TO source_name;')
            if 'src_config' in existing_columns and 'source_config' not in existing_columns:
                cursor.execute(f'ALTER TABLE "{main_schema}".source RENAME COLUMN src_config TO source_config;')

            # Create Destination table in the GENERAL schema
            # Include project_id for project-based organization
            destination_table_sql = '''
            CREATE TABLE IF NOT EXISTS "{main_schema}".destination (
                id SERIAL PRIMARY KEY,
                dest_name VARCHAR(255),
                dest_config TEXT,
                project_id INTEGER,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );
            '''
            cursor.execute(destination_table_sql)

            # Create Source Table Selection table in the GENERAL schema
            # This stores selected tables from source connections with fields in JSON format
            source_table_selection_sql = '''
            CREATE TABLE IF NOT EXISTS "{main_schema}".source_table_selection (
                tbl_id SERIAL PRIMARY KEY,
                source_id INTEGER NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                schema VARCHAR(100),
                table_fields JSONB,
                selected BOOLEAN DEFAULT TRUE,
                added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_synced TIMESTAMP,
                UNIQUE(source_id, table_name, schema)
            );
            '''
            cursor.execute(source_table_selection_sql)

            sequence_table_sql = '''
            CREATE TABLE IF NOT EXISTS "{main_schema}".tbl_col_seq (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100),
                table_name VARCHAR(100),
                sequence VARCHAR(400),
                seq_name VARCHAR(100),
                scope VARCHAR(10) CHECK (scope IN ('G', 'L'))
            );
            '''
            cursor.execute(sequence_table_sql)

            # Create CANVAS_CACHE schema for node transformation caching
            cache_schema = "CANVAS_CACHE"
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{cache_schema}";')

            # Create cache metadata table
            cache_metadata_sql = '''
            CREATE TABLE IF NOT EXISTS "{cache_schema}".node_cache_metadata (
                id SERIAL PRIMARY KEY,
                canvas_id INTEGER NOT NULL,
                node_id VARCHAR(100) NOT NULL,
                node_name VARCHAR(255),
                node_type VARCHAR(50) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                config_hash VARCHAR(64),
                row_count INTEGER DEFAULT 0,
                column_count INTEGER DEFAULT 0,
                columns JSONB,
                source_node_ids JSONB,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN DEFAULT TRUE,
                UNIQUE(canvas_id, node_id)
            );
            '''
            cursor.execute(cache_metadata_sql)

            # Create index for faster lookups
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_node_cache_lookup
                ON "{cache_schema}".node_cache_metadata (canvas_id, node_id);
            ''')

            print(f"Created schemas and tables in database {self.cust_db}: {main_schema}, {cache_schema}")

        except Exception as e:
            print(f"Error creating schemas in database {self.cust_db}: {e!s}")
            raise Exception(f"Failed to create schemas in customer database: {e!s}")

        finally:
            # Ensure connections are always closed
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    class Meta:
        db_table = 'customer'

class ObjectMap(models.Model):
    object_id = models.CharField(max_length=100)
    tecname = models.CharField(max_length=100)
    object_nme = models.CharField(max_length=100)
    tname = models.CharField(max_length=100)

    class Meta:
        db_table = 'objectmap'

class ValidationRules(models.Model):
    question = models.CharField(max_length=200)
    expression = models.CharField(max_length=200)
    category = models.CharField(max_length=100, null=True)

    class Meta:
        db_table = 'validationrules'

class Roles(models.Model):
    role_id = models.AutoField(primary_key=True)
    cust_id = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name='customer_roles')
    role_name = models.CharField(max_length=100)
    created_on = models.DateTimeField(default=timezone.now)
    is_active = models.BooleanField(default=True)

    # need to add these field when template is created for the role crud operation
    # created_by
    # modified_by
    # modified_on

    def __str__(self):
        return self.role_name

    class Meta:
        db_table = 'roles'

class UsrRoles(models.Model):
    usr_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey(User, on_delete=models.CASCADE, related_name='user_role')
    role_id = models.ForeignKey(Roles, on_delete=models.CASCADE)
    created_on = models.DateTimeField(default=timezone.now)
    valid_from = models.DateField(null=True)
    valid_to = models.DateField(null=True)

    # need to add these field when template is created for the role crud operation
    # created_by
    # modified_by
    # modified_on

    class Meta:
        db_table = 'usrroles'

# Source and Destination Models for Dynamic Form Generation
class SourceModel(models.Model):
    """Model representing source database types (PostgreSQL, MySQL, Oracle, SQL Server)"""
    src_id = models.PositiveIntegerField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    dst_schema = models.CharField(max_length=100, blank=True, null=True)

    def save(self, *args, **kwargs):
        if self.pk is None:
            last_src_id = SourceModel.objects.aggregate(max_src_id=models.Max('src_id'))['max_src_id']
            if last_src_id is not None:
                self.src_id = last_src_id + 1
            else:
                self.src_id = 1
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'source_model'

class SourceAttribute(models.Model):
    """Model representing form fields/attributes for each source type"""
    TEXT_INPUT = 'TextInput'
    PASSWORD_INPUT = 'PasswordInput'
    SELECT = 'Select'
    TEXTAREA = 'Textarea'

    WIDGET_CHOICES = [
        (TEXT_INPUT, 'TextInput'),
        (PASSWORD_INPUT, 'PasswordInput'),
        (SELECT, 'Select'),
        (TEXTAREA, 'Textarea'),
    ]

    src_attr_id = models.IntegerField(primary_key=True)
    src = models.ForeignKey('SourceModel', on_delete=models.CASCADE, related_name='attributes')
    attribute_name = models.CharField(max_length=100)
    input_type = models.CharField(max_length=50)
    label = models.CharField(max_length=100)
    widget = models.CharField(max_length=50, choices=WIDGET_CHOICES, blank=True, null=True)
    required = models.BooleanField(default=True)
    is_visible = models.BooleanField(default=True)
    choices = models.JSONField(null=True, blank=True)
    depend_on = models.CharField(max_length=100, null=True, blank=True)
    dependency_value = models.CharField(max_length=100, null=True, blank=True)

    def save(self, *args, **kwargs):
        if self.pk is None:
            last_src_attr_id = SourceAttribute.objects.aggregate(max_src_attr_id=models.Max('src_attr_id'))['max_src_attr_id']
            if last_src_attr_id is not None:
                self.src_attr_id = last_src_attr_id + 1
            else:
                self.src_attr_id = 1
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.src.name} - {self.attribute_name}"

    class Meta:
        db_table = 'source_attribute'

class SourceConfig(models.Model):
    """Model storing encrypted source connection configurations"""
    src_config_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    src = models.ForeignKey('SourceModel', on_delete=models.CASCADE, related_name='configs')
    cmp = models.ForeignKey('Customer', on_delete=models.SET_NULL, null=True, related_name='source_configs')
    config_data = models.TextField(null=True)  # Encrypted field
    is_active = models.BooleanField(default=False)
    last_synced = models.DateTimeField(auto_now_add=True)
    response_message = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'source_config'
        unique_together = ['cmp', 'name']

    @property
    def data(self):
        """Decrypt and return the config data."""
        return self._decrypt_field(self.config_data)

    @data.setter
    def data(self, value):
        """Temporarily store `data` for encryption during save."""
        if value is not None:
            self._data_to_encrypt = value
        else:
            self._data_to_encrypt = None
            self.config_data = None

    def _decrypt_field(self, encrypted_value):
        """Decrypt the encrypted field."""
        if not encrypted_value:
            return None

        try:
            import json

            from api.connections.encryption import decrypt_field

            encrypted_value = json.loads(encrypted_value)
            (encrypted_data, nonce, tag, salt, original_type, iterations) = encrypted_value

            if not self.cmp:
                return None

            created_day = self.cmp.created_on.day
            incremented_value = int(self.cmp.cust_id.replace('C', '')) if self.cmp.cust_id.startswith('C') else 0
            incremented_value = incremented_value + created_day

            return decrypt_field(
                encrypted_data, incremented_value, nonce, tag, salt, original_type
            )
        except (ValueError, KeyError, TypeError) as e:
            print(f"Decryption error: {e}")
            return None

    def __str__(self):
        return f"{self.name} ({self.src.name})"

class DestinationModel(models.Model):
    """Model representing destination database types (HANA DB)"""
    dst_id = models.PositiveIntegerField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    min_count = models.PositiveIntegerField(default=0)
    max_count = models.PositiveIntegerField(default=500)

    def save(self, *args, **kwargs):
        if not self.dst_id:
            last_dst_id = DestinationModel.objects.aggregate(max_dst_id=models.Max('dst_id'))['max_dst_id']
            if last_dst_id is not None:
                self.dst_id = last_dst_id + 1
            else:
                self.dst_id = 1
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'destination_model'

class DestinationAttribute(models.Model):
    """Model representing form fields/attributes for each destination type"""
    CHAR_FIELD = 'CharField'
    BOOLEAN_FIELD = 'BooleanField'
    DATETIME_FIELD = 'DateTimeField'
    DURATION_FIELD = 'DurationField'
    CHOICE_FIELD = 'ChoiceField'
    PASSWORD_FIELD = 'PasswordField'
    TEXTAREA_FIELD = 'TextAreaField'

    INPUT_TYPE_CHOICES = [
        (CHAR_FIELD, 'CharField'),
        (BOOLEAN_FIELD, 'BooleanField'),
        (DATETIME_FIELD, 'DateTimeField'),
        (DURATION_FIELD, 'DurationField'),
        (CHOICE_FIELD, 'ChoiceField'),
        (PASSWORD_FIELD, 'PasswordField'),
        (TEXTAREA_FIELD, 'TextAreaField'),
    ]

    TEXT_INPUT = 'TextInput'
    PASSWORD_INPUT = 'PasswordInput'
    SELECT = 'Select'
    TEXTAREA = 'Textarea'

    WIDGET_CHOICES = [
        (TEXT_INPUT, 'TextInput'),
        (PASSWORD_INPUT, 'PasswordInput'),
        (SELECT, 'Select'),
        (TEXTAREA, 'Textarea'),
    ]

    dst_attr_id = models.IntegerField(primary_key=True)
    dst = models.ForeignKey('DestinationModel', on_delete=models.CASCADE, related_name='attributes')
    attribute_name = models.CharField(max_length=255)
    input_type = models.CharField(max_length=100, choices=INPUT_TYPE_CHOICES)
    label = models.CharField(max_length=255)
    choices = models.JSONField(blank=True, null=True)
    widget = models.CharField(max_length=50, choices=WIDGET_CHOICES, blank=True, null=True)
    required = models.BooleanField(default=True)
    is_visible = models.BooleanField(default=True)
    depend_on = models.CharField(max_length=255, blank=True, null=True)
    dependency = models.CharField(max_length=255, blank=True, null=True)

    def save(self, *args, **kwargs):
        if self.pk is None:
            last_dst_attr_id = DestinationAttribute.objects.aggregate(
                max_dst_attr_id=models.Max('dst_attr_id')
            )['max_dst_attr_id']
            self.dst_attr_id = (last_dst_attr_id + 1) if last_dst_attr_id is not None else 1
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.dst.name} - {self.attribute_name}"

    class Meta:
        db_table = 'destination_attribute'

class DestinationConfig(models.Model):
    """Model storing encrypted destination connection configurations"""
    dst_config_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    dst = models.ForeignKey('DestinationModel', on_delete=models.CASCADE, related_name='configs')
    cmp = models.ForeignKey('Customer', on_delete=models.SET_NULL, null=True, related_name='destination_configs')
    config_data = models.TextField(null=True)  # Encrypted field
    is_active = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    response_message = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'destination_config'
        unique_together = ['cmp', 'name']

    @property
    def data(self):
        """Decrypt and return the config data."""
        return self._decrypt_field(self.config_data)

    @data.setter
    def data(self, value):
        """Encrypt immediately so .save() works on update too."""
        if value is not None:
            import json

            from api.connections.encryption import encrypt_field

            if not self.cmp:
                return

            created_day = self.cmp.created_on.day
            incremented_value = int(self.cmp.cust_id.replace('C', '')) if self.cmp.cust_id.startswith('C') else 0
            incremented_value = incremented_value + created_day

            encrypted_data = encrypt_field(value, incremented_value)
            self.config_data = json.dumps(encrypted_data)
        else:
            self.config_data = None

    def _decrypt_field(self, encrypted_value):
        """Decrypt the encrypted field."""
        if not encrypted_value:
            return None

        try:
            import json

            from api.connections.encryption import decrypt_field

            encrypted_value = json.loads(encrypted_value)
            (encrypted_data, nonce, tag, salt, original_type, iterations) = encrypted_value

            if not self.cmp:
                return None

            created_day = self.cmp.created_on.day
            incremented_value = int(self.cmp.cust_id.replace('C', '')) if self.cmp.cust_id.startswith('C') else 0
            incremented_value = incremented_value + created_day

            return decrypt_field(
                encrypted_data, incremented_value, nonce, tag, salt, original_type
            )
        except (ValueError, KeyError, TypeError) as e:
            print(f"Decryption error: {e}")
            return None

    def __str__(self):
        return f"{self.name} ({self.dst.name})"

# Signal handlers for encryption
from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender=SourceConfig)
def encrypt_source_config_data(sender, instance, created, **kwargs):
    """Encrypt the `data` property of SourceConfig after saving."""
    if hasattr(instance, '_data_to_encrypt') and not getattr(instance, '_encrypting', False):
        instance._encrypting = True
        try:
            import json

            from api.connections.encryption import encrypt_field

            if not instance.cmp:
                return

            created_day = instance.cmp.created_on.day
            incremented_value = int(instance.cmp.cust_id.replace('C', '')) if instance.cmp.cust_id.startswith('C') else 0
            incremented_value = incremented_value + created_day

            encrypted_data = encrypt_field(instance._data_to_encrypt, incremented_value)
            instance.config_data = json.dumps(encrypted_data)
            instance.save(update_fields=['config_data'])
        finally:
            instance._encrypting = False

@receiver(post_save, sender=DestinationConfig)
def encrypt_destination_config_data(sender, instance, created, **kwargs):
    """Encrypt the `data` property of DestinationConfig after saving."""
    if hasattr(instance, '_data_to_encrypt') and not getattr(instance, '_encrypting', False):
        instance._encrypting = True
        try:
            import json

            from api.connections.encryption import encrypt_field

            if not instance.cmp:
                return

            created_day = instance.cmp.created_on.day
            incremented_value = int(instance.cmp.cust_id.replace('C', '')) if instance.cmp.cust_id.startswith('C') else 0
            incremented_value = incremented_value + created_day

            encrypted_data = encrypt_field(instance._data_to_encrypt, incremented_value)
            instance.config_data = json.dumps(encrypted_data)
            instance.save(update_fields=['config_data'])
        finally:
            instance._encrypting = False
