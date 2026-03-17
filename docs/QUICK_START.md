# Quick Start Guide

## Step 1: Update PostgreSQL Password

**Important:** Update the PostgreSQL password in `datamigrationapi/settings.py` (line 86) with your actual password.

Or set it as an environment variable:
```powershell
$env:DB_PASSWORD="your_password_here"
```

## Step 2: Create Database and Schema

Run the database setup script (update password in the script first):
```powershell
cd datamigration-migcockpit
python create_db_simple.py
```

Or manually in psql:
```sql
CREATE DATABASE datamigrate;
\c datamigrate
CREATE SCHEMA IF NOT EXISTS "GENERAL";
```

## Step 3: Run Migrations

```powershell
python manage.py migrate
```

## Step 4: Create Superuser

```powershell
python create_superuser.py
```

Default credentials:
- Email: `admin@example.com`
- Password: `admin123`

## Step 5: Start Django Server

```powershell
python manage.py runserver 8000
```

## Step 6: Start Frontend

Open a new terminal:
```powershell
cd datamigration-migcockpit\frontend
npm run dev
```

## Step 7: Access the Interface

Open browser: **http://localhost:3000**

Login with:
- Email: `admin@example.com`
- Password: `admin123`

