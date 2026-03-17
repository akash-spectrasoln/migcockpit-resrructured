# Migration Guide: Old Structure to New Structure

## Overview

This guide documents the migration from the old project structure to the new refactored structure. It helps developers understand what changed and how to update their code.

---

## Summary of Changes

### Backend Changes

1. **Models**: Consolidated from `api/models.py` → `api/models/` (domain-organized)
2. **Views**: Split from `api/views.py` → `api/views/` (domain-organized)
3. **Services**: Moved from root → `api/services/`
4. **Configuration**: Moved from hardcoded → environment variables

### Frontend Changes

1. **Components**: Better organization in `components/Canvas/`
2. **Services**: Centralized API clients
3. **Types**: Centralized type definitions

---

## Import Pattern Changes

### Models

#### Old Pattern
```python
from api.models import User, Customer, Canvas
# or
from .models import User, Customer
```

#### New Pattern
```python
# Still works! (via __init__.py)
from api.models import User, Customer, Canvas

# Or direct import
from api.models.base import User, Customer
from api.models.canvas import Canvas
```

**Action Required**: No changes needed if using `from api.models import X`. Direct imports from `models.py` need updating.

### Views

#### Old Pattern
```python
from api.views import LoginView, SourceConnectionCreateView
# or
from .views import LoginView
```

#### New Pattern
```python
# Still works! (via __init__.py)
from api.views import LoginView, SourceConnectionCreateView

# Or direct import
from api.views.auth import LoginView
from api.views.connections import SourceConnectionCreateView
```

**Action Required**: No changes needed if using `from api.views import X`. Direct imports from `views.py` need updating.

### Services

#### Old Pattern
```python
from encryption.encryption import encrypt_field, decrypt_field
from fetch_sqlserver.fetch_sqldata import extract_data
```

#### New Pattern
```python
from api.services import encrypt_field, decrypt_field, extract_data
# or
from api.services.encryption_service import encrypt_field
from api.services.sqlserver_connector import extract_data
```

**Action Required**: Update all service imports.

---

## File Location Changes

### Models

| Old Location | New Location | Notes |
|-------------|--------------|-------|
| `api/models.py` (all models) | `api/models/base.py` | Base models |
| `api/models.py` (Canvas) | `api/models/canvas.py` | Already existed |
| `api/models.py` (Migration) | `api/models/migration_job.py` | Already existed |
| `api/models.py` (Project) | `api/models/project.py` | Already existed |

### Views

| Old Location | New Location | Notes |
|-------------|--------------|-------|
| `api/views.py` (Auth) | `api/views/auth.py` | New file |
| `api/views.py` (Connections) | `api/views/connections.py` | New file |
| `api/views.py` (Tables) | `api/views/tables.py` | New file |
| `api/views.py` (Users) | `api/views/users.py` | New file |
| `api/views.py` (Utils) | `api/views/utils.py` | New file |
| `api/views/canvas_views.py` | `api/views/canvas_views.py` | No change |
| `api/views/migration_views.py` | `api/views/migration_views.py` | No change |

### Services

| Old Location | New Location | Notes |
|-------------|--------------|-------|
| `encryption/encryption.py` | `api/services/encryption_service.py` | Moved |
| `fetch_sqlserver/fetch_sqldata.py` | `api/services/sqlserver_connector.py` | Moved |
| `hana_connection/` | `api/services/hana_connector.py` | Consolidated |

---

## Step-by-Step Migration

### Step 1: Update Service Imports

Find and replace all service imports:

```bash
# Find all occurrences
grep -r "from encryption" .
grep -r "from fetch_sqlserver" .
grep -r "from hana_connection" .

# Update each file
```

**Example Update:**

```python
# Before
from encryption.encryption import encrypt_field

# After
from api.services import encrypt_field
# or
from api.services.encryption_service import encrypt_field
```

### Step 2: Update Model Imports (if using direct imports)

If you have direct imports from `models.py`:

```python
# Before
from api.models import User  # This still works!

# If you had:
from ..models import User  # Relative import

# Update to:
from api.models import User  # Absolute import
```

### Step 3: Update View Imports (if using direct imports)

If you have direct imports from `views.py`:

```python
# Before
from api.views import LoginView  # This still works!

# If you had:
from ..views import LoginView  # Relative import

# Update to:
from api.views import LoginView  # Absolute import
```

### Step 4: Update Configuration

#### Environment Variables

Create `.env` file:

```env
DATABASE_NAME=datamigrate
DATABASE_USER=postgres
DATABASE_PASSWORD=your_password
SECRET_KEY=your-secret-key
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1
```

Update `settings.py`:

```python
# Before
DATABASES = {
    'default': {
        'NAME': 'datamigrate',
        'USER': 'postgres',
        'PASSWORD': 'SecurePassword123!',
        # ...
    }
}

# After
from decouple import config

DATABASES = {
    'default': {
        'NAME': config('DATABASE_NAME'),
        'USER': config('DATABASE_USER'),
        'PASSWORD': config('DATABASE_PASSWORD'),
        # ...
    }
}
```

---

## Common Migration Scenarios

### Scenario 1: Adding a New Model

**Old Way:**
```python
# api/models.py
class NewModel(models.Model):
    # ...
```

**New Way:**
```python
# api/models/base.py (or appropriate domain file)
class NewModel(models.Model):
    # ...

# api/models/__init__.py
from .base import NewModel
__all__ = [..., 'NewModel']
```

### Scenario 2: Adding a New View

**Old Way:**
```python
# api/views.py
class NewView(APIView):
    # ...
```

**New Way:**
```python
# api/views/utils.py (or appropriate domain file)
class NewView(APIView):
    # ...

# api/views/__init__.py
from .utils import NewView
__all__ = [..., 'NewView']
```

### Scenario 3: Using Encryption Service

**Old Way:**
```python
from encryption.encryption import encrypt_field, decrypt_field

encrypted = encrypt_field(value, cust_id, created_on)
```

**New Way:**
```python
from api.services import encrypt_field, decrypt_field

encrypted = encrypt_field(value, cust_id, created_on)
```

---

## Testing After Migration

### 1. Run Backend Tests

```bash
python manage.py test
```

### 2. Test API Endpoints

```bash
# Test authentication
curl -X POST http://localhost:8000/api/api-login/ \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "password": "password"}'

# Test other endpoints
```

### 3. Test Frontend

```bash
cd frontend
npm run dev
# Open browser and test functionality
```

### 4. Check Admin Panel

```bash
# Start server
python manage.py runserver

# Open http://localhost:8000/admin/
# Verify all models are accessible
```

---

## Troubleshooting

### Issue: Import Errors

**Error**: `ModuleNotFoundError: No module named 'encryption'`

**Solution**: Update import to use new location:
```python
# Old
from encryption.encryption import encrypt_field

# New
from api.services import encrypt_field
```

### Issue: Model Not Found

**Error**: `django.core.exceptions.AppRegistryNotReady: Apps aren't loaded yet`

**Solution**: Ensure models are properly exported in `api/models/__init__.py`

### Issue: View Not Found

**Error**: `AttributeError: module 'api.views' has no attribute 'LoginView'`

**Solution**: Ensure view is exported in `api/views/__init__.py`

### Issue: Environment Variables Not Loading

**Error**: `decouple.UndefinedValueError: DATABASE_NAME not found`

**Solution**: 
1. Create `.env` file from `.env.example`
2. Install `python-decouple`: `pip install python-decouple`
3. Verify `.env` file is in project root

---

## Rollback Plan

If issues occur, you can rollback:

1. **Git Rollback**
   ```bash
   git checkout <previous-commit>
   ```

2. **Keep Old Files Temporarily**
   - Don't delete `models.py` and `views.py` until migration is complete
   - They can coexist during transition

3. **Gradual Migration**
   - Migrate one module at a time
   - Test after each migration
   - Commit after each successful migration

---

## Migration Checklist

- [ ] Update all service imports (`encryption`, `fetch_sqlserver`, `hana_connection`)
- [ ] Update any direct model imports (if using relative imports)
- [ ] Update any direct view imports (if using relative imports)
- [ ] Create `.env` file from `.env.example`
- [ ] Update `settings.py` to use environment variables
- [ ] Run backend tests: `python manage.py test`
- [ ] Test API endpoints
- [ ] Test frontend application
- [ ] Verify admin panel works
- [ ] Update any custom scripts that reference old paths
- [ ] Update documentation references
- [ ] Notify team of changes

---

## Post-Migration

After successful migration:

1. **Remove Old Files** (only after everything works):
   - `api/models.py` (if empty)
   - `api/views.py` (if empty)
   - `encryption/` directory
   - `fetch_sqlserver/` directory
   - `hana_connection/` directory

2. **Update Documentation**:
   - Update any references to old structure
   - Update onboarding documentation

3. **Team Communication**:
   - Notify team of new import patterns
   - Share migration guide
   - Update team wiki/docs

---

## Questions?

If you encounter issues during migration:

1. Check this guide
2. Review `PROJECT_STRUCTURE.md` for new structure
3. Check `DEVELOPER_GUIDE.md` for patterns
4. Ask team lead or senior developers
5. Check git history for migration commits

---

## Additional Resources

- **Project Structure**: `docs/PROJECT_STRUCTURE.md`
- **Developer Guide**: `docs/DEVELOPER_GUIDE.md`
- **Django Migration Docs**: https://docs.djangoproject.com/en/4.2/topics/migrations/

