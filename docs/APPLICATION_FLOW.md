# Data Migration Cockpit - Application Flow

## Complete User Journey Flowchart

```mermaid
flowchart TD
    Start([User Opens Application]) --> CheckAuth{Token in<br/>LocalStorage?}
    
    CheckAuth -->|No Token| LoginPage[Login Page]
    CheckAuth -->|Has Token| VerifyToken{Verify Token<br/>Valid?}
    
    VerifyToken -->|Invalid| LoginPage
    VerifyToken -->|Valid| CanvasPage[Canvas Page]
    
    LoginPage --> EnterCredentials[Enter Email & Password]
    EnterCredentials --> SubmitLogin[Submit Login Form]
    SubmitLogin --> APILogin[POST /api-login/<br/>Django REST API]
    
    APILogin -->|Success| StoreTokens[Store JWT Tokens<br/>access_token & refresh_token]
    StoreTokens --> SetAuthState[Set isAuthenticated = true]
    SetAuthState --> NavigateCanvas[Navigate to /canvas]
    
    APILogin -->|Failure| ShowError[Show Error Message]
    ShowError --> LoginPage
    
    NavigateCanvas --> LoadCanvas[Load Canvas Data]
    LoadCanvas --> CheckExistingCanvas{Canvas<br/>Exists?}
    
    CheckExistingCanvas -->|Yes| LoadCanvasConfig[Load Nodes & Edges<br/>from Database]
    CheckExistingCanvas -->|No| EmptyCanvas[Empty Canvas Ready]
    
    LoadCanvasConfig --> CanvasReady[Canvas Ready]
    EmptyCanvas --> CanvasReady
    
    CanvasReady --> CanvasView[Canvas View Mode]
    
    CanvasView --> UserAction{User Action}
    
    UserAction -->|Drag Node| AddNode[Add Node to Canvas]
    UserAction -->|Click Node| OpenConfig[Open Configuration Panel]
    UserAction -->|Connect Nodes| CreateEdge[Create Edge Connection]
    UserAction -->|Click Save| SaveCanvas[Save Canvas to DB]
    UserAction -->|Click Validate| ValidatePipeline[Validate Pipeline]
    UserAction -->|Click Execute| ExecuteMigration[Execute Migration]
    UserAction -->|Click Jobs Button| NavigateJobs[Navigate to /jobs]
    UserAction -->|Click Logout| Logout[Logout User]
    
    AddNode --> NodePalette[Select from Node Palette]
    NodePalette --> SourceNode[Source Node<br/>MySQL/Oracle/SQL Server]
    NodePalette --> TransformNode[Transform Node<br/>Map/Filter/Clean/Validate]
    NodePalette --> DestNode[Destination Node<br/>SAP HANA]
    
    SourceNode --> ConfigureNode
    TransformNode --> ConfigureNode
    DestNode --> ConfigureNode
    
    ConfigureNode[Configure Node] --> ConfigPanel[Configuration Panel]
    ConfigPanel --> FillForm[Fill Configuration Form]
    FillForm --> SaveConfig[Save Configuration]
    SaveConfig --> CanvasView
    
    CreateEdge --> ValidateConnection{Valid<br/>Connection?}
    ValidateConnection -->|Yes| AddEdge[Add Edge to Canvas]
    ValidateConnection -->|No| ShowError2[Show Connection Error]
    AddEdge --> CanvasView
    ShowError2 --> CanvasView
    
    SaveCanvas --> APISave[POST /api/canvas/{id}/save-configuration/]
    APISave -->|Success| ShowSuccess[Show Success Message]
    APISave -->|Error| ShowError3[Show Error Message]
    ShowSuccess --> CanvasView
    ShowError3 --> CanvasView
    
    ValidatePipeline --> APIValidate[POST /api/metadata/validate_pipeline/]
    APIValidate --> ValidationResult{Validation<br/>Result}
    
    ValidationResult -->|Valid| ShowValid[Show Validation Success]
    ValidationResult -->|Invalid| ShowErrors[Show Validation Errors]
    ShowValid --> CanvasView
    ShowErrors --> CanvasView
    
    ExecuteMigration --> CheckSaved{Canvas<br/>Saved?}
    CheckSaved -->|No| PromptSave[Prompt to Save First]
    PromptSave --> CanvasView
    
    CheckSaved -->|Yes| APICreateJob[POST /api/migration-jobs/execute/]
    APICreateJob --> CreateJobRecord[Create MigrationJob Record<br/>in Django DB]
    
    CreateJobRecord --> CallMigrationService[Call Migration Service<br/>FastAPI Port 8003]
    CallMigrationService --> StartBackgroundTask[Start Background Task<br/>Celery Worker]
    
    StartBackgroundTask --> BuildPipeline[Build Pipeline from Canvas]
    BuildPipeline --> ExecutePipeline[Execute Pipeline]
    
    ExecutePipeline --> ExtractData[Extract Data<br/>Extraction Service Port 8001]
    ExtractData --> TransformData[Transform Data<br/>Transformation Service Port 8002]
    TransformData --> LoadToHANA[Load to HANA<br/>Migration Service]
    
    LoadToHANA --> UpdateProgress[Update Job Progress]
    UpdateProgress --> BroadcastWS[Broadcast via WebSocket<br/>Port 8004]
    BroadcastWS --> UpdateUI[Update UI in Real-Time]
    
    UpdateUI --> JobComplete{Job<br/>Complete?}
    JobComplete -->|No| UpdateProgress
    JobComplete -->|Yes| FinalStatus[Update Final Status]
    
    NavigateJobs --> JobsPage[Jobs Monitoring Page]
    JobsPage --> LoadJobs[Load Jobs from API<br/>GET /api/migration-jobs/]
    LoadJobs --> DisplayJobs[Display Job List]
    
    DisplayJobs --> JobActions{Job Actions}
    JobActions -->|Select Job| ShowJobDetails[Show Job Details Sidebar]
    JobActions -->|Filter Jobs| ApplyFilters[Apply Filters<br/>Status/Search/Date]
    JobActions -->|Cancel Job| CancelJob[POST /api/migration-jobs/{id}/cancel/]
    JobActions -->|Back to Canvas| NavigateCanvas2[Navigate to /canvas]
    
    ShowJobDetails --> LoadJobLogs[Load Job Logs<br/>GET /api/migration-jobs/{id}/logs/]
    LoadJobLogs --> DisplayLogs[Display Logs in Sidebar]
    
    JobsPage --> WSSubscribe[Subscribe to WebSocket<br/>for Running Jobs]
    WSSubscribe --> ReceiveUpdates[Receive Real-Time Updates]
    ReceiveUpdates --> UpdateJobList[Update Job List UI]
    UpdateJobList --> UpdateNodeStatus[Update Node Status on Canvas]
    
    Logout --> ClearTokens[Clear Tokens from<br/>LocalStorage]
    ClearTokens --> ClearAuthState[Set isAuthenticated = false]
    ClearAuthState --> NavigateLogin[Navigate to /login]
    NavigateLogin --> LoginPage
    
    style Start fill:#e1f5ff
    style LoginPage fill:#fff3cd
    style CanvasPage fill:#d4edda
    style JobsPage fill:#d1ecf1
    style ExecuteMigration fill:#f8d7da
    style UpdateUI fill:#d4edda
    style Logout fill:#f8d7da
```

## Detailed Flow Explanation

### 1. Authentication Flow

```
┌─────────────┐
│   Start     │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ Check Auth Token │
└──────┬───────────┘
       │
       ├─── No Token ────► Login Page
       │
       └─── Has Token ────► Verify Token
                              │
                              ├─── Invalid ────► Login Page
                              │
                              └─── Valid ────► Canvas Page
```

**Steps:**
1. User opens application → Check for token in localStorage
2. No token → Redirect to Login Page
3. Has token → Verify token validity via API call
4. Invalid token → Clear tokens, redirect to Login
5. Valid token → Proceed to Canvas Page

### 2. Login Flow

```
Login Page
    │
    ├─── Enter Email & Password
    │
    ├─── Submit Form
    │
    ├─── POST /api-login/
    │    │
    │    ├─── Success
    │    │    ├─── Store access_token & refresh_token
    │    │    ├─── Set isAuthenticated = true
    │    │    └─── Navigate to /canvas
    │    │
    │    └─── Failure
    │         └─── Show Error Message
```

**API Flow:**
- Frontend: `POST /api-login/` with `{email, password}`
- Django: Validates credentials, returns JWT tokens
- Frontend: Stores tokens, updates auth state
- Navigate: Redirects to `/canvas`

### 3. Canvas Page Flow

```
Canvas Page Loads
    │
    ├─── Check Authentication
    │    └─── Not Authenticated ────► Redirect to /login
    │
    ├─── Load Canvas Data
    │    ├─── GET /api/canvas/
    │    │
    │    ├─── Canvas Exists
    │    │    └─── Load nodes & edges from configuration
    │    │
    │    └─── No Canvas
    │         └─── Empty canvas ready
    │
    └─── Render Canvas
         ├─── Node Palette (Left Side)
         ├─── Canvas Area (Center)
         └─── Configuration Panel (Right Side - when node selected)
```

### 4. Canvas Workflow

```
Canvas View
    │
    ├─── DESIGN MODE
    │    │
    │    ├─── Drag Node from Palette
    │    │    └─── Node appears on canvas
    │    │
    │    ├─── Click Node
    │    │    └─── Open Configuration Panel
    │    │         ├─── Fill configuration form
    │    │         └─── Save configuration
    │    │
    │    ├─── Connect Nodes
    │    │    └─── Drag from output handle to input handle
    │    │
    │    ├─── Save Canvas
    │    │    └─── POST /api/canvas/{id}/save-configuration/
    │    │
    │    └─── Validate Pipeline
    │         └─── POST /api/metadata/validate_pipeline/
    │
    ├─── VALIDATE MODE
    │    │
    │    └─── Show Validation Results
    │         ├─── Errors (if any)
    │         └─── Warnings (if any)
    │
    └─── EXECUTE
         └─── Start Migration Job
```

### 5. Migration Execution Flow

```
Execute Button Clicked
    │
    ├─── Check Canvas Saved
    │    └─── Not Saved ────► Prompt to Save
    │
    ├─── Validate Pipeline
    │    └─── Has Errors ────► Show Errors, Stop
    │
    ├─── POST /api/migration-jobs/execute/
    │    │
    │    ├─── Django Creates MigrationJob Record
    │    │
    │    ├─── Calls Migration Service (FastAPI)
    │    │    └─── POST http://localhost:8003/execute
    │    │
    │    └─── Returns Job ID
    │
    ├─── Subscribe to WebSocket
    │    └─── wsService.subscribeToJobUpdates(jobId)
    │
    └─── Navigate to Monitor View
         │
         └─── Real-Time Updates via WebSocket
              ├─── Overall job progress
              ├─── Per-node status
              └─── Logs streaming
```

### 6. Job Execution Pipeline

```
Migration Service Receives Request
    │
    ├─── Build Pipeline (Topological Sort)
    │    └─── Determine execution order
    │
    ├─── For Each Node (in order):
    │    │
    │    ├─── SOURCE NODE
    │    │    ├─── Call Extraction Service
    │    │    │    └─── POST http://localhost:8001/extract
    │    │    ├─── Extract data in chunks
    │    │    └─── Store extracted data
    │    │
    │    ├─── TRANSFORM NODE
    │    │    ├─── Get data from previous nodes
    │    │    ├─── Call Transformation Service
    │    │    │    └─── POST http://localhost:8002/transform
    │    │    ├─── Apply transformations
    │    │    └─── Store transformed data
    │    │
    │    └─── DESTINATION NODE
    │         ├─── Get data from previous nodes
    │         ├─── Load to SAP HANA
    │         └─── Update job status
    │
    └─── Broadcast Progress Updates
         └─── POST http://localhost:8004/broadcast/{job_id}
```

### 7. Real-Time Updates Flow

```
WebSocket Connection
    │
    ├─── Frontend Connects
    │    └─── io.connect('http://localhost:8004')
    │
    ├─── Join Job Room
    │    └─── socket.emit('join_job', { job_id })
    │
    ├─── Migration Service Broadcasts
    │    └─── POST /broadcast/{job_id}
    │         └─── WebSocket Server emits to room
    │
    └─── Frontend Receives Updates
         ├─── 'status' event ────► Update job status
         ├─── 'node_progress' event ────► Update node status
         ├─── 'complete' event ────► Mark job complete
         ├─── 'error' event ────► Show error
         └─── 'cancelled' event ────► Mark cancelled
```

### 8. Jobs Page Flow

```
Jobs Page Loads
    │
    ├─── Check Authentication
    │    └─── Not Authenticated ────► Redirect to /login
    │
    ├─── Load Jobs
    │    └─── GET /api/migration-jobs/
    │
    ├─── Display Job List
    │    ├─── Table with filters
    │    └─── Job details sidebar (when selected)
    │
    ├─── Subscribe to WebSocket (for running jobs)
    │    └─── Real-time updates
    │
    └─── User Actions
         ├─── Filter Jobs ────► Apply filters, refresh list
         ├─── Select Job ────► Show details, load logs
         ├─── Cancel Job ────► POST /api/migration-jobs/{id}/cancel/
         └─── Back to Canvas ────► Navigate to /canvas
```

## Navigation Map

```
                    ┌─────────────┐
                    │   Start     │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  Login Page │
                    └──────┬──────┘
                           │ (Login Success)
                           ▼
        ┌──────────────────────────────────┐
        │         Canvas Page               │
        │  ┌──────────┐  ┌──────────────┐   │
        │  │  Node    │  │   Canvas     │   │
        │  │ Palette  │  │   Area       │   │
        │  └──────────┘  └──────────────┘   │
        │         │              │          │
        │         └──────┬───────┘          │
        │                │                   │
        │         ┌──────▼──────┐           │
        │         │  Configure   │           │
        │         │    Node      │           │
        │         └──────────────┘           │
        └─────┬──────────────────┬───────────┘
              │                  │
              │ (Jobs Button)    │ (Logout)
              ▼                  ▼
        ┌─────────────┐    ┌─────────────┐
        │ Jobs Page   │    │  Login Page │
        └──────┬──────┘    └─────────────┘
               │
               │ (Back to Canvas)
               ▼
        ┌─────────────┐
        │ Canvas Page │
        └─────────────┘
```

## State Management Flow

```
┌─────────────────────────────────────────┐
│         Global State Stores             │
├─────────────────────────────────────────┤
│                                         │
│  AuthStore (Zustand)                    │
│  ├─── isAuthenticated: boolean          │
│  ├─── token: string | null              │
│  ├─── login(email, password)            │
│  ├─── logout()                          │
│  └─── checkAuth()                       │
│                                         │
│  CanvasStore (Zustand)                  │
│  ├─── nodes: Node[]                     │
│  ├─── edges: Edge[]                    │
│  ├─── selectedNode: Node | null         │
│  ├─── viewMode: 'design' | 'validate'   │
│  │                      | 'monitor'     │
│  ├─── activeJobId: string | null       │
│  ├─── jobProgress: Record<string, num>  │
│  └─── nodeStatuses: Record<string, ...> │
│                                         │
└─────────────────────────────────────────┘
           │              │
           │              │
           ▼              ▼
    ┌──────────┐    ┌──────────┐
    │  Login   │    │  Canvas  │
    │   Page   │    │   Page   │
    └──────────┘    └──────────┘
```

## API Call Flow

```
Frontend (React)
    │
    ├─── Authentication
    │    └─── POST /api-login/ ────► Django REST API
    │
    ├─── Canvas Operations
    │    ├─── GET /api/canvas/ ────► Django REST API
    │    ├─── POST /api/canvas/ ────► Django REST API
    │    └─── POST /api/canvas/{id}/save-configuration/ ────► Django REST API
    │
    ├─── Migration Jobs
    │    ├─── POST /api/migration-jobs/execute/ ────► Django REST API
    │    │    └─── Calls ────► Migration Service (FastAPI :8003)
    │    │
    │    ├─── GET /api/migration-jobs/ ────► Django REST API
    │    ├─── GET /api/migration-jobs/{id}/status/ ────► Django REST API
    │    │    └─── Queries ────► Migration Service (FastAPI :8003)
    │    │
    │    └─── GET /api/migration-jobs/{id}/logs/ ────► Django REST API
    │
    ├─── Metadata
    │    ├─── GET /api/metadata/tables/ ────► Django REST API
    │    │    └─── Calls ────► Extraction Service (FastAPI :8001)
    │    │
    │    ├─── GET /api/metadata/columns/ ────► Django REST API
    │    │    └─── Calls ────► Extraction Service (FastAPI :8001)
    │    │
    │    └─── POST /api/metadata/validate_pipeline/ ────► Django REST API
    │
    └─── WebSocket
         └─── ws://localhost:8004 ────► WebSocket Server
              └─── Receives broadcasts from Migration Service
```

## Complete User Journey

### Step-by-Step Flow

1. **Initial Access**
   - User opens http://localhost:3000
   - App checks for authentication token
   - No token → Redirect to `/login`

2. **Login Process**
   - User enters email and password
   - Clicks "Login" button
   - Frontend calls `POST /api-login/`
   - Django validates credentials
   - Returns JWT tokens (access + refresh)
   - Frontend stores tokens in localStorage
   - Sets `isAuthenticated = true`
   - Navigates to `/canvas`

3. **Canvas Page**
   - Checks authentication (if not authenticated → `/login`)
   - Loads existing canvas from API
   - If canvas exists → Loads nodes and edges
   - If no canvas → Shows empty canvas
   - Renders:
     - **Left Panel**: Node Palette (Source/Transform/Destination)
     - **Center**: Canvas area with React Flow
     - **Right Panel**: Configuration panel (when node selected)
     - **Top Toolbar**: View modes, Save, Validate, Execute buttons

4. **Building Pipeline**
   - **Add Source Node**: Drag from palette → Configure connection & table
   - **Add Transform Node**: Drag from palette → Configure rules
   - **Add Destination Node**: Drag from palette → Configure HANA connection
   - **Connect Nodes**: Drag from output handle to input handle
   - **Save Canvas**: Saves configuration to database

5. **Validating Pipeline**
   - Click "Validate" button
   - Frontend validates locally
   - Calls `POST /api/metadata/validate_pipeline/`
   - Django validates:
     - At least one source and destination
     - Required configurations
     - Graph connectivity
   - Shows errors/warnings if any

6. **Executing Migration**
   - Click "Execute" button
   - Checks canvas is saved
   - Validates pipeline
   - Calls `POST /api/migration-jobs/execute/`
   - Django creates MigrationJob record
   - Calls Migration Service (FastAPI)
   - Returns job_id
   - Frontend subscribes to WebSocket for job_id
   - Navigates to Monitor view or Jobs page

7. **Job Execution (Backend)**
   - Migration Service builds pipeline
   - Executes nodes in topological order:
     - Source → Extract data
     - Transform → Transform data
     - Destination → Load to HANA
   - Broadcasts progress via WebSocket
   - Updates job status in database

8. **Real-Time Monitoring**
   - WebSocket receives updates
   - Updates job status in UI
   - Updates per-node progress
   - Shows logs in real-time
   - Updates canvas node statuses

9. **Jobs Page**
   - Lists all migration jobs
   - Filters by status/search/date
   - Shows job details when selected
   - Displays logs for running jobs
   - Allows canceling running jobs
   - Navigation back to Canvas

10. **Logout**
    - Click "Logout" button
    - Clears tokens from localStorage
    - Sets `isAuthenticated = false`
    - Navigates to `/login`

## Key Navigation Points

- **Login** → **Canvas** (after successful login)
- **Canvas** → **Jobs** (via "Jobs" button in header)
- **Jobs** → **Canvas** (via "Back to Canvas" button)
- **Canvas** → **Login** (via "Logout" button)
- **Any Protected Route** → **Login** (if not authenticated)

## Protected Routes

All routes except `/login` are protected:
- `/canvas` - Requires authentication
- `/jobs` - Requires authentication
- `/` - Redirects to `/canvas` (requires authentication)

## State Persistence

- **Authentication**: Tokens stored in localStorage
- **Canvas State**: Stored in Zustand store (in-memory)
- **Canvas Configuration**: Persisted in PostgreSQL database
- **Job State**: Stored in PostgreSQL database, synced via WebSocket

## Error Handling Flow

```
Error Occurs
    │
    ├─── API Error
    │    ├─── 401 Unauthorized ────► Refresh Token ────► Retry
    │    │    └─── Refresh Fails ────► Logout ────► Login Page
    │    │
    │    ├─── 400 Bad Request ────► Show Error Message
    │    ├─── 404 Not Found ────► Show Error Message
    │    └─── 500 Server Error ────► Show Error Message
    │
    ├─── WebSocket Error
    │    └─── Disconnect ────► Auto Reconnect ────► Fallback to Polling
    │
    └─── Validation Error
         └─── Show Validation Errors in UI
```

This flowchart and explanation covers the complete application flow from login through canvas operations to job monitoring!

