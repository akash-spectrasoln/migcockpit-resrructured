# Developer Guide

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Setup](#development-setup)
3. [Code Organization](#code-organization)
4. [Common Tasks](#common-tasks)
5. [Code Patterns](#code-patterns)
6. [Testing Guidelines](#testing-guidelines)
7. [Debugging](#debugging)
8. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Prerequisites

- Python 3.10+
- Node.js 18+
- PostgreSQL 12+
- Git

### Initial Setup

1. **Clone Repository**
   ```bash
   git clone <repository-url>
   cd migcockpit-qoder/migcockpit/datamigration-migcockpit
   ```

2. **Backend Setup**
   ```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Copy environment template
   cp .env.example .env
   # Edit .env with your configuration
   
   # Run migrations
   python manage.py migrate
   
   # Create superuser
   python manage.py createsuperuser
   ```

3. **Frontend Setup**
   ```bash
   cd frontend
   npm install
   ```

4. **Start Development Servers**
   ```bash
   # Backend (Terminal 1)
   python manage.py runserver
   
   # Frontend (Terminal 2)
   cd frontend
   npm run dev
   ```

---

## Development Setup

### IDE Configuration

#### VS Code

Recommended extensions:
- Python
- Pylance
- ESLint
- Prettier
- Django

#### PyCharm

- Configure Django project
- Set up Python interpreter
- Configure database connections

### Environment Variables

Create `.env` file from `.env.example`:

```env
# Database
DATABASE_NAME=datamigrate
DATABASE_USER=postgres
DATABASE_PASSWORD=your_password
DATABASE_HOST=localhost
DATABASE_PORT=5433

# Django
SECRET_KEY=your-secret-key-here
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1

# CORS
CORS_ALLOWED_ORIGINS=http://localhost:5173
```

---

## Code Organization

### Backend Structure

```
api/
├── models/          # Data models (domain-organized)
├── views/           # API views (domain-organized)
├── serializers/     # DRF serializers
├── services/        # Business logic
├── utils/          # Utility functions
└── permissions.py   # Custom permissions
```

### Frontend Structure

```
frontend/src/
├── components/      # React components
├── pages/          # Page components
├── hooks/          # Custom hooks
├── services/       # API services
├── store/          # State management
├── types/          # TypeScript types
└── utils/          # Utility functions
```

---

## Common Tasks

### Adding a New API Endpoint

#### Step 1: Create Model (if needed)

```python
# api/models/base.py
class NewResource(models.Model):
    name = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'new_resource'
```

#### Step 2: Create Serializer

```python
# api/serializers/base_serializers.py
class NewResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = NewResource
        fields = ['id', 'name', 'created_at']
```

#### Step 3: Create View

```python
# api/views/utils.py (or appropriate domain file)
from rest_framework.views import APIView
from rest_framework.response import Response
from api.models import NewResource
from api.serializers import NewResourceSerializer

class NewResourceView(APIView):
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        resources = NewResource.objects.all()
        serializer = NewResourceSerializer(resources, many=True)
        return Response(serializer.data)
    
    def post(self, request):
        serializer = NewResourceSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
```

#### Step 4: Export View

```python
# api/views/__init__.py
from .utils import NewResourceView
__all__ = [..., 'NewResourceView']
```

#### Step 5: Add URL Route

```python
# api/urls.py
from .views import NewResourceView

urlpatterns = [
    # ...
    path('new-resource/', NewResourceView.as_view(), name='new-resource'),
]
```

#### Step 6: Create Migration

```bash
python manage.py makemigrations
python manage.py migrate
```

### Adding a New Frontend Component

#### Step 1: Create Component

```typescript
// frontend/src/components/shared/NewComponent.tsx
import React from 'react'
import { Box, Text } from '@chakra-ui/react'

interface NewComponentProps {
  title: string
  onAction?: () => void
}

export const NewComponent: React.FC<NewComponentProps> = ({ 
  title, 
  onAction 
}) => {
  return (
    <Box p={4}>
      <Text fontSize="lg" fontWeight="bold">{title}</Text>
      {onAction && (
        <Button onClick={onAction}>Action</Button>
      )}
    </Box>
  )
}
```

#### Step 2: Export Component

```typescript
// frontend/src/components/shared/index.ts
export { NewComponent } from './NewComponent'
```

#### Step 3: Use Component

```typescript
// In your page/component
import { NewComponent } from '@/components/shared'

export const MyPage = () => {
  return <NewComponent title="Hello" />
}
```

### Adding a New Service Function

#### Step 1: Create Service File (if new)

```python
# api/services/new_service.py
def process_data(data):
    """Process data and return result."""
    # Business logic here
    result = perform_processing(data)
    return result
```

#### Step 2: Export Service

```python
# api/services/__init__.py
from .new_service import process_data
__all__ = [..., 'process_data']
```

#### Step 3: Use in View

```python
# api/views/utils.py
from api.services import process_data

class ProcessDataView(APIView):
    def post(self, request):
        result = process_data(request.data)
        return Response({'result': result})
```

---

## Code Patterns

### Backend Patterns

#### ViewSet Pattern (REST Framework)

```python
# api/views/resource_views.py
from rest_framework import viewsets
from api.models import Resource
from api.serializers import ResourceSerializer

class ResourceViewSet(viewsets.ModelViewSet):
    queryset = Resource.objects.all()
    serializer_class = ResourceSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        # Custom filtering
        queryset = super().get_queryset()
        if self.request.query_params.get('active'):
            queryset = queryset.filter(is_active=True)
        return queryset
```

#### Service Pattern

```python
# api/services/resource_service.py
from api.models import Resource

class ResourceService:
    @staticmethod
    def create_resource(data):
        """Create resource with validation."""
        # Business logic
        resource = Resource.objects.create(**data)
        # Post-creation logic
        return resource
    
    @staticmethod
    def process_resource(resource_id):
        """Process resource."""
        resource = Resource.objects.get(id=resource_id)
        # Processing logic
        return resource
```

#### Error Handling Pattern

```python
# api/views/utils.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging

logger = logging.getLogger(__name__)

class SafeView(APIView):
    def post(self, request):
        try:
            # Operation
            result = perform_operation(request.data)
            return Response({'success': True, 'data': result})
        except ValueError as e:
            logger.warning(f"Validation error: {e}")
            return Response(
                {'error': str(e)}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return Response(
                {'error': 'Internal server error'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
```

### Frontend Patterns

#### Custom Hook Pattern

```typescript
// frontend/src/hooks/useResource.ts
import { useState, useEffect } from 'react'
import { api } from '@/services/api'

export const useResource = (resourceId: string) => {
  const [resource, setResource] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetchResource = async () => {
      try {
        setLoading(true)
        const response = await api.get(`/resource/${resourceId}`)
        setResource(response.data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    if (resourceId) {
      fetchResource()
    }
  }, [resourceId])

  return { resource, loading, error }
}
```

#### API Service Pattern

```typescript
// frontend/src/services/resourceApi.ts
import { api } from './api'

export const resourceApi = {
  list: () => api.get('/resources/'),
  get: (id: string) => api.get(`/resources/${id}/`),
  create: (data: any) => api.post('/resources/', data),
  update: (id: string, data: any) => api.put(`/resources/${id}/`, data),
  delete: (id: string) => api.delete(`/resources/${id}/`),
}
```

#### Component with State Pattern

```typescript
// frontend/src/components/ResourceList.tsx
import React, { useState, useEffect } from 'react'
import { Box, Spinner, Alert } from '@chakra-ui/react'
import { resourceApi } from '@/services/resourceApi'

export const ResourceList: React.FC = () => {
  const [resources, setResources] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetchResources = async () => {
      try {
        const response = await resourceApi.list()
        setResources(response.data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    fetchResources()
  }, [])

  if (loading) return <Spinner />
  if (error) return <Alert status="error">{error}</Alert>

  return (
    <Box>
      {resources.map(resource => (
        <Box key={resource.id}>{resource.name}</Box>
      ))}
    </Box>
  )
}
```

---

## Testing Guidelines

### Backend Testing

#### Unit Tests

```python
# api/tests/test_models.py
from django.test import TestCase
from api.models import User, Customer

class UserModelTest(TestCase):
    def setUp(self):
        self.customer = Customer.objects.create(name="Test Customer")
    
    def test_create_user(self):
        user = User.objects.create(
            email="test@example.com",
            first_name="Test",
            last_name="User",
            created_by="admin",
            cust_id=self.customer
        )
        self.assertEqual(user.email, "test@example.com")
```

#### API Tests

```python
# api/tests/test_views.py
from rest_framework.test import APITestCase
from rest_framework import status
from api.models import User

class LoginViewTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
            created_by="admin"
        )
    
    def test_login_success(self):
        response = self.client.post('/api/api-login/', {
            'email': 'test@example.com',
            'password': 'testpass123'
        })
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('access', response.data)
```

### Frontend Testing

#### Component Tests

```typescript
// frontend/src/__tests__/components/NewComponent.test.tsx
import { render, screen } from '@testing-library/react'
import { NewComponent } from '@/components/shared/NewComponent'

describe('NewComponent', () => {
  it('renders title', () => {
    render(<NewComponent title="Test Title" />)
    expect(screen.getByText('Test Title')).toBeInTheDocument()
  })
})
```

### Running Tests

```bash
# Backend
python manage.py test

# Frontend
cd frontend
npm test
```

---

## Debugging

### Backend Debugging

#### Django Debug Toolbar

Add to `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    # ...
    'debug_toolbar',
]
```

#### Logging

```python
import logging

logger = logging.getLogger(__name__)

def my_function():
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
```

### Frontend Debugging

#### React DevTools

Install browser extension for React DevTools

#### Console Logging

```typescript
console.log('Debug info:', data)
console.error('Error:', error)
```

#### Debugger

```typescript
function myFunction() {
  debugger  // Breakpoint
  // Code here
}
```

---

## Troubleshooting

### Common Issues

#### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'api.models'`

**Solution**: 
- Check Python path
- Ensure virtual environment is activated
- Verify `api` is in `INSTALLED_APPS`

#### Database Connection Errors

**Problem**: `django.db.utils.OperationalError: could not connect to server`

**Solution**:
- Check PostgreSQL is running
- Verify `.env` configuration
- Check database credentials

#### Frontend Build Errors

**Problem**: TypeScript errors or build failures

**Solution**:
- Run `npm install` to update dependencies
- Check TypeScript configuration
- Verify all imports are correct

#### CORS Errors

**Problem**: CORS policy blocking requests

**Solution**:
- Check `CORS_ALLOWED_ORIGINS` in settings
- Verify frontend URL is in allowed origins
- Check CORS middleware is enabled

---

## Best Practices

### Code Quality

1. **Follow PEP 8** (Python) and **ESLint** (TypeScript)
2. **Write Docstrings** for functions and classes
3. **Use Type Hints** (Python) and **TypeScript** types
4. **Keep Functions Small** and focused
5. **Avoid Deep Nesting** (max 3-4 levels)

### Git Workflow

1. **Create Feature Branch**: `git checkout -b feature/new-feature`
2. **Commit Frequently**: Small, logical commits
3. **Write Descriptive Messages**: "Add user authentication endpoint"
4. **Test Before Committing**: Run tests locally
5. **Push Regularly**: Keep remote branch updated

### Code Review

1. **Self-Review First**: Review your own code before requesting review
2. **Be Constructive**: Provide helpful feedback
3. **Respond Promptly**: Address review comments quickly
4. **Test Changes**: Verify fixes work as expected

---

## Additional Resources

- **Project Structure**: `docs/PROJECT_STRUCTURE.md`
- **Migration Guide**: `docs/MIGRATION_GUIDE.md`
- **API Documentation**: `docs/API_Documentation.xlsx`
- **Django Docs**: https://docs.djangoproject.com/
- **React Docs**: https://react.dev/
- **TypeScript Docs**: https://www.typescriptlang.org/docs/

---

## Getting Help

1. Check this guide first
2. Search existing documentation
3. Ask team members
4. Check error logs and stack traces
5. Consult framework documentation

