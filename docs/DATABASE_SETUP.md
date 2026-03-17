# PostgreSQL Database Setup Guide

## Quick Setup

### Option 1: Update Password in settings.py

1. Open `datamigrationapi/settings.py`
2. Find the `DATABASES` configuration (around line 81)
3. Update the password to match your PostgreSQL password:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'datamigrate',
        'USER': 'postgres',
        'PASSWORD': 'YOUR_POSTGRES_PASSWORD_HERE',  # Update this
        'HOST': 'localhost',  
        'PORT': '5432', 
        'OPTIONS': {
            'options': '-c search_path="GENERAL"'
        } 
    }
}
```

### Option 2: Use Environment Variables

Create a `.env` file in the project root:

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_NAME=datamigrate
```

Then update `settings.py` to read from environment:

```python
import os
from dotenv import load_dotenv

load_dotenv()

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('DB_NAME', 'datamigrate'),
        'USER': os.getenv('DB_USER', 'postgres'),
        'PASSWORD': os.getenv('DB_PASSWORD', 'Password@123'),
        'HOST': os.getenv('DB_HOST', 'localhost'),
        'PORT': os.getenv('DB_PORT', '5432'),
        'OPTIONS': {
            'options': '-c search_path="GENERAL"'
        } 
    }
}
```

## Create Database

### Using the Setup Script

1. Update the password in `setup_database.py` (line 12) or run it interactively
2. Run: `python setup_database.py`

### Manual Setup (psql)

Connect to PostgreSQL:

```bash
psql -U postgres -h localhost
```

Then run:

```sql
CREATE DATABASE datamigrate;
\c datamigrate
CREATE SCHEMA IF NOT EXISTS "GENERAL";
\q
```

### Using Python

```python
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to default postgres database
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    password='YOUR_PASSWORD',
    database='postgres'
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Create database
cursor.execute('CREATE DATABASE datamigrate;')
print("Database created!")

# Switch to new database and create schema
conn.close()
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    password='YOUR_PASSWORD',
    database='datamigrate'
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute('CREATE SCHEMA IF NOT EXISTS "GENERAL";')
print("Schema created!")

cursor.close()
conn.close()
```

## Run Migrations

After the database is created, run:

```bash
python manage.py migrate
```

## Verify Connection

Test the connection:

```bash
python manage.py dbshell
```

Or use Python:

```python
python manage.py shell
>>> from django.db import connection
>>> connection.ensure_connection()
>>> print("Connected!")
```

## Troubleshooting

### "password authentication failed"
- Check your PostgreSQL password
- Verify the user exists: `SELECT * FROM pg_user WHERE usename = 'postgres';`
- Check `pg_hba.conf` for authentication settings

### "database does not exist"
- Run the database creation script
- Or create manually using psql

### "connection refused"
- Ensure PostgreSQL is running: `pg_ctl status` or check Windows services
- Verify port 5432 is not blocked by firewall
- Check PostgreSQL is listening on localhost

### "schema does not exist"
- The schema will be created automatically on first migration
- Or create manually: `CREATE SCHEMA IF NOT EXISTS "GENERAL";`

