"""
Django management command to migrate existing data to project-based structure.

This command:
1. Creates the projects table if it doesn't exist
2. Adds project_id column to canvas table if it doesn't exist
3. Creates a default project for each existing customer
4. Assigns existing canvases to default projects
5. Optionally adds project_id to source/destination tables in customer databases
"""

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
import psycopg2

from api.models import Customer
from api.models.canvas import Canvas


class Command(BaseCommand):
    help = 'Migrate existing data to project-based structure'

    def add_arguments(self, parser):
        parser.add_argument(
            '--skip-customer-dbs',
            action='store_true',
            help='Skip adding project_id to customer database tables (sources/destinations)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        skip_customer_dbs = options['skip_customer_dbs']

        self.stdout.write(self.style.SUCCESS('Starting project migration...'))

        # Step 1: Create projects table
        self.stdout.write('\n1. Creating projects table...')
        self.create_projects_table(dry_run)

        # Step 2: Add project_id to canvas table
        self.stdout.write('\n2. Adding project_id to canvas table...')
        self.add_project_id_to_canvas(dry_run)

        # Step 3: Create default projects for customers
        self.stdout.write('\n3. Creating default projects for customers...')
        default_projects = self.create_default_projects(dry_run)

        # Step 4: Assign existing canvases to default projects
        self.stdout.write('\n4. Assigning existing canvases to default projects...')
        self.assign_canvases_to_projects(default_projects, dry_run)

        # Step 5: Add project_id to customer database tables (optional)
        if not skip_customer_dbs:
            self.stdout.write('\n5. Adding project_id to customer database tables...')
            self.add_project_id_to_customer_tables(dry_run)
        else:
            self.stdout.write('\n5. Skipping customer database table updates (--skip-customer-dbs)')

        self.stdout.write(self.style.SUCCESS('\n✓ Migration completed successfully!'))

    def create_projects_table(self, dry_run):
        """Create projects table if it doesn't exist"""
        with connection.cursor() as cursor:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'projects'
                )
            """)
            table_exists = cursor.fetchone()[0]

            if table_exists:
                self.stdout.write('  ✓ Projects table already exists')
                return

            if dry_run:
                self.stdout.write('  [DRY RUN] Would create projects table')
                return

            create_sql = '''
            CREATE TABLE projects (
                id SERIAL PRIMARY KEY,
                project_name VARCHAR(255) NOT NULL,
                description TEXT,
                customer_id INTEGER NOT NULL REFERENCES customer(cust_id) ON DELETE CASCADE,
                created_by_id INTEGER,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_by_id INTEGER,
                is_active BOOLEAN DEFAULT TRUE
            );
            '''
            cursor.execute(create_sql)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_projects_customer_id ON projects(customer_id);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_projects_is_active ON projects(is_active);')
            self.stdout.write(self.style.SUCCESS('  ✓ Created projects table'))

    def add_project_id_to_canvas(self, dry_run):
        """Add project_id column to canvas table if it doesn't exist"""
        with connection.cursor() as cursor:
            # Check if column exists
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'canvas' AND column_name = 'project_id'
            """)
            column_exists = cursor.fetchone() is not None

            if column_exists:
                self.stdout.write('  ✓ project_id column already exists in canvas table')
                return

            if dry_run:
                self.stdout.write('  [DRY RUN] Would add project_id column to canvas table')
                return

            cursor.execute('ALTER TABLE canvas ADD COLUMN project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL;')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_canvas_project_id ON canvas(project_id);')
            self.stdout.write(self.style.SUCCESS('  ✓ Added project_id column to canvas table'))

    def create_default_projects(self, dry_run):
        """Create a default project for each customer"""
        customers = Customer.objects.all()
        default_projects = {}

        for customer in customers:
            with connection.cursor() as cursor:
                # Check if default project already exists
                cursor.execute("""
                    SELECT id FROM projects
                    WHERE customer_id = %s AND project_name = 'Default Project'
                    LIMIT 1
                """, [customer.pk])
                existing = cursor.fetchone()

                if existing:
                    project_id = existing[0]
                    self.stdout.write(f'  ✓ Default project already exists for customer {customer.cust_id}')
                    default_projects[customer.pk] = project_id
                    continue

                if dry_run:
                    self.stdout.write(f'  [DRY RUN] Would create default project for customer {customer.cust_id}')
                    default_projects[customer.pk] = None
                    continue

                cursor.execute("""
                    INSERT INTO projects (project_name, description, customer_id, is_active)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, [
                    'Default Project',
                    'Default project for existing data',
                    customer.pk,
                    True
                ])
                project_id = cursor.fetchone()[0]
                default_projects[customer.pk] = project_id
                self.stdout.write(self.style.SUCCESS(f'  ✓ Created default project for customer {customer.cust_id} (ID: {project_id})'))

        return default_projects

    def assign_canvases_to_projects(self, default_projects, dry_run):
        """Assign existing canvases without project_id to default projects"""
        canvases_without_project = Canvas.objects.filter(project__isnull=True)
        count = 0

        for canvas in canvases_without_project:
            customer_id = canvas.customer.pk
            project_id = default_projects.get(customer_id)

            if not project_id:
                self.stdout.write(self.style.WARNING(f'  ⚠ No default project for customer {customer_id}, skipping canvas {canvas.id}'))
                continue

            if dry_run:
                self.stdout.write(f'  [DRY RUN] Would assign canvas {canvas.id} to project {project_id}')
                count += 1
                continue

            canvas.project_id = project_id
            canvas.save(update_fields=['project_id'])
            count += 1

        if count > 0:
            self.stdout.write(self.style.SUCCESS(f'  ✓ Assigned {count} canvas(es) to default projects'))
        else:
            self.stdout.write('  ✓ No canvases needed assignment')

    def add_project_id_to_customer_tables(self, dry_run):
        """Add project_id column to source and destination tables in customer databases"""
        customers = Customer.objects.all()
        total_updated = 0

        for customer in customers:
            try:
                conn = psycopg2.connect(
                    host=settings.DATABASES['default']['HOST'],
                    port=settings.DATABASES['default']['PORT'],
                    user=settings.DATABASES['default']['USER'],
                    password=settings.DATABASES['default']['PASSWORD'],
                    database=customer.cust_db
                )
                conn.autocommit = True
                cursor = conn.cursor()

                # Check and add project_id to source table
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'source' AND column_name = 'project_id'
                """)
                if cursor.fetchone() is None:
                    if dry_run:
                        self.stdout.write(f'  [DRY RUN] Would add project_id to GENERAL.source in {customer.cust_db}')
                    else:
                        cursor.execute('ALTER TABLE "GENERAL".source ADD COLUMN project_id INTEGER;')
                        self.stdout.write(self.style.SUCCESS(f'  ✓ Added project_id to GENERAL.source in {customer.cust_db}'))
                        total_updated += 1

                # Check and add project_id to destination table
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'GENERAL' AND table_name = 'destination' AND column_name = 'project_id'
                """)
                if cursor.fetchone() is None:
                    if dry_run:
                        self.stdout.write(f'  [DRY RUN] Would add project_id to GENERAL.destination in {customer.cust_db}')
                    else:
                        cursor.execute('ALTER TABLE "GENERAL".destination ADD COLUMN project_id INTEGER;')
                        self.stdout.write(self.style.SUCCESS(f'  ✓ Added project_id to GENERAL.destination in {customer.cust_db}'))
                        total_updated += 1

                cursor.close()
                conn.close()

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'  ✗ Error updating {customer.cust_db}: {e!s}'))

        if total_updated > 0:
            self.stdout.write(self.style.SUCCESS(f'  ✓ Updated {total_updated} customer database(s)'))
        else:
            self.stdout.write('  ✓ All customer databases already have project_id columns')
