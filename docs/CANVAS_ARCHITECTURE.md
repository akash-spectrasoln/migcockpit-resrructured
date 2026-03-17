# Enhanced Canvas Architecture Documentation

## Overview

This document describes the enhanced Canvas architecture for the Data Migration Cockpit. The system provides a visual, drag-and-drop interface for building data migration pipelines with extensible node types, schema-driven configuration, and real-time monitoring.

## Architecture Components

### 1. Frontend Architecture (React + TypeScript)

#### State Management (`frontend/src/store/`)

- **`canvasStore.ts`**: Global Zustand store managing:
  - Canvas graph (nodes, edges)
  - Selected node/edge
  - View mode (design/validate/run/monitor)
  - Job state and progress
  - Node statuses

#### Node Type Registry (`frontend/src/types/nodeRegistry.ts`)

Extensible system for registering node types:

```typescript
// Register a new node type
registerNodeType({
  id: 'source-postgres',
  category: 'source',
  label: 'PostgreSQL Source',
  description: 'Extract data from PostgreSQL',
  icon: 'Database',
  color: 'blue',
  defaultConfig: { ... },
  configSchema: [ ... ],
})
```

**Current Node Types:**
- **Sources**: MySQL, Oracle, SQL Server
- **Transforms**: Map, Filter, Clean, Validate
- **Destinations**: SAP HANA

#### Canvas Components (`frontend/src/components/Canvas/`)

- **`EnhancedDataFlowCanvas.tsx`**: Main canvas with:
  - React Flow integration
  - View mode switching
  - Validation panel
  - Toolbar with actions

- **`NodePalette.tsx`**: Draggable node palette organized by category

- **`NodeConfigurationPanel.tsx`**: Schema-driven configuration form:
  - Auto-generates form fields from node type schema
  - Validates required fields
  - Supports: text, number, select, textarea, checkbox, JSON

- **`NodeTypes.tsx`**: Visual node components (Source, Transform, Destination)

- **`EdgeTypes.tsx`**: Custom edge styling

#### Pages (`frontend/src/pages/`)

- **`CanvasPage.tsx`**: Main canvas page with header and navigation
- **`JobsPage.tsx`**: Migration jobs monitoring:
  - Job list with filters
  - Real-time status updates
  - Job details sidebar
  - Logs viewer

#### Services (`frontend/src/services/`)

- **`api.ts`**: Axios-based API client with:
  - JWT authentication
  - Token refresh
  - Canvas, Migration, Connection, Metadata APIs

- **`websocket.ts`**: WebSocket client for real-time updates (to be enhanced)

### 2. Backend Architecture (Django REST API)

#### Models (`api/models/`)

- **`canvas.py`**: Canvas, CanvasNode, CanvasEdge models
- **`migration_job.py`**: MigrationJob, MigrationJobLog models

#### Views (`api/views/`)

- **`canvas_views.py`**: Canvas CRUD operations
- **`migration_views.py`**: Migration job management:
  - Execute migration
  - Get status
  - Get logs
  - Cancel job

- **`metadata_views.py`**: Metadata endpoints:
  - Get tables for source
  - Get columns for table
  - Get validation rules
  - Validate pipeline

#### Serializers (`api/serializers/`)

- Canvas serializers
- Migration job serializers

### 3. FastAPI Microservices

- **Extraction Service** (Port 8001): Data extraction from source databases
- **Transformation Service** (Port 8002): Data transformations
- **Migration Service** (Port 8003): Pipeline orchestration and HANA loading

## Key Features

### 1. Extensibility

**Adding a New Node Type:**

1. **Backend**: Add connector/transformer/loader in appropriate FastAPI service
2. **Frontend**: Register node type in `nodeRegistry.ts`:
   ```typescript
   registerNodeType({
     id: 'source-postgres',
     category: 'source',
     label: 'PostgreSQL Source',
     // ... config schema
   })
   ```
3. **Frontend**: Node automatically appears in palette and is configurable

**Adding a New Filter/Transform Rule:**

1. **Backend**: Add rule definition to metadata API
2. **Frontend**: Rule automatically available in transform node configuration

### 2. Schema-Driven Configuration

Node configurations are driven by schemas defined in the node registry:

```typescript
configSchema: [
  {
    name: 'tableName',
    label: 'Table Name',
    type: 'text',
    required: true,
    validation: (value) => value ? null : 'Required'
  }
]
```

The `NodeConfigurationPanel` automatically renders appropriate form fields.

### 3. Pipeline Validation

Validation happens at multiple levels:

- **Frontend**: Client-side validation before save/execute
- **Backend**: Server-side validation via `/api/metadata/validate_pipeline/`

Validates:
- At least one source and destination
- Required node configurations
- Graph connectivity
- Edge validity

### 4. Real-Time Monitoring

- **WebSocket**: For small jobs (< 1GB), real-time updates
- **Polling**: For large jobs, REST API polling every 3 seconds
- **Job Status**: Per-node progress and status tracking

## Usage Guide

### Creating a Migration Pipeline

1. **Design Phase**:
   - Drag source nodes from palette
   - Configure source connections and tables
   - Add transform nodes (map, filter, clean, validate)
   - Configure transformations
   - Add destination node
   - Connect nodes with edges

2. **Validate Phase**:
   - Click "Validate" button
   - Review validation errors/warnings
   - Fix any issues

3. **Execute Phase**:
   - Click "Execute" button
   - Job is created and queued
   - Redirected to Monitor view

4. **Monitor Phase**:
   - View job list
   - Select job to see details
   - Monitor progress and logs
   - Cancel if needed

### API Usage

**Canvas Operations:**
```typescript
// Get all canvases
const canvases = await canvasApi.getAll()

// Create canvas
const canvas = await canvasApi.create({
  name: 'My Pipeline',
  configuration: { nodes: [], edges: [] }
})

// Save configuration
await canvasApi.saveConfiguration(canvasId, { configuration })
```

**Migration Jobs:**
```typescript
// Execute migration
const job = await migrationApi.execute(canvasId, {
  nodes: [...],
  edges: [...]
})

// Get job status
const status = await migrationApi.getStatus(jobId)

// Get logs
const logs = await migrationApi.getLogs(jobId)
```

**Metadata:**
```typescript
// Get tables for source
const tables = await metadataApi.getTables(sourceId)

// Get columns for table
const columns = await metadataApi.getColumns(sourceId, tableName)

// Validate pipeline
const validation = await metadataApi.validatePipeline(nodes, edges)
```

## File Structure

```
frontend/src/
├── components/Canvas/
│   ├── EnhancedDataFlowCanvas.tsx  # Main canvas
│   ├── NodePalette.tsx             # Node palette
│   ├── NodeConfigurationPanel.tsx  # Config panel
│   ├── NodeTypes.tsx               # Node components
│   └── EdgeTypes.tsx               # Edge components
├── pages/
│   ├── CanvasPage.tsx              # Canvas page
│   └── JobsPage.tsx                # Jobs monitoring
├── services/
│   ├── api.ts                      # API client
│   └── websocket.ts                # WebSocket client
├── store/
│   ├── canvasStore.ts              # Canvas state
│   └── authStore.ts                # Auth state
└── types/
    └── nodeRegistry.ts              # Node type registry

api/
├── models/
│   ├── canvas.py
│   └── migration_job.py
├── views/
│   ├── canvas_views.py
│   ├── migration_views.py
│   └── metadata_views.py
└── serializers/
    ├── canvas_serializers.py
    └── migration_serializers.py
```

## Best Practices

1. **Node Configuration**: Always define complete config schemas with validation
2. **Error Handling**: Use try-catch and show user-friendly error messages
3. **State Management**: Use Zustand store for global state, local state for UI-only
4. **API Calls**: Use the centralized API service, not direct axios calls
5. **Type Safety**: Use TypeScript interfaces for all data structures
6. **Extensibility**: When adding features, consider how they fit into the registry pattern

## Future Enhancements

1. **WebSocket Integration**: Complete real-time updates for job progress
2. **Pydantic Models**: Add typed request/response models to FastAPI services
3. **Advanced Validation**: Cycle detection, dependency validation
4. **Node Templates**: Save and reuse common node configurations
5. **Pipeline Versioning**: Track changes to pipelines over time
6. **Scheduling**: Schedule recurring migrations
7. **Data Preview**: Preview data at each node in the pipeline

## Troubleshooting

**Canvas not loading:**
- Check Django server is running on port 8000
- Check authentication token is valid
- Check browser console for errors

**Node configuration not saving:**
- Verify node has required fields filled
- Check API response in network tab
- Verify canvas ID is set

**Jobs not executing:**
- Check FastAPI services are running
- Verify Celery workers are running
- Check Redis connection
- Review job logs in Django admin

**Validation errors:**
- Ensure all source nodes have sourceId and tableName
- Ensure all destination nodes have destinationId and tableName
- Check graph connectivity (nodes are connected)

