# Frontend Architecture - Data Migration Cockpit

This frontend is a React + TypeScript application for building, validating, and executing migration pipelines on a visual canvas.

## Stack

- React 18 + TypeScript
- Vite
- Chakra UI
- React Flow (graph canvas)
- Zustand (canvas/auth client state)
- TanStack React Query (server state)
- Axios (HTTP API layer)

## High-Level Architecture

The app is organized around four layers:

1. **Presentation Layer**
   - Pages (`src/pages`)
   - Canvas components (`src/components/canvas`)
   - Config panels (`src/components/canvas/panels`)
   - Sidebar/table browser (`src/components/canvas/sidebar`)

2. **State Layer**
   - Global UI/canvas state in `src/store/canvasStore.ts`
   - Auth/session state in `src/store/authStore.ts`
   - Query cache + async orchestration via React Query hooks

3. **Pipeline Engine Layer**
   - Graph/schema normalization and inference in `src/pipeline-engine`
   - Validation helpers in `src/pipeline-engine/validator.ts`
   - Column mapping/schema logic in `src/pipeline-engine/schema.ts`

4. **Integration Layer**
   - API route constants: `src/constants/server-routes.ts`
   - Axios clients: `src/lib/axios/api-client.ts`
   - Domain hooks: `src/hooks/useMigration.ts`, `src/hooks/useConnections.ts`, `src/hooks/useSchemaDrift.ts`

## Core Runtime Flow

1. User designs a graph in `DataFlowCanvas`.
2. Node/edge/config changes are persisted in `canvasStore`.
3. Pipeline engine derives graph metadata/column context for panels and validation.
4. Frontend calls validate endpoint (`/validate`) using the API layer.
5. Backend returns validation result + plan metadata.
6. On execute, frontend triggers migration (`/execute`) and tracks progress via polling/websocket updates.
7. UI updates node/job statuses and table previews in near real-time.

## Canvas Subsystem

Main entry points:

- `src/pages/CanvasPage.tsx`
- `src/components/canvas/DataFlowCanvas.tsx`
- `src/components/canvas/panels/NodeConfigPanel.tsx`

Supporting modules:

- `nodes/`: source/filter/join/projection/compute node renderers
- `interactions/`: edge menus, context actions, insert/destination controls
- `panels/`: per-node configuration UIs (filter, join, projection, destination, etc.)
- `sidebar/`: source connections and table browser (Remote/Repository sections)

## Data and API Boundaries

- **Do not call APIs directly from deep UI components** when possible; prefer domain hooks or typed client methods.
- API client contracts live in:
  - `src/constants/server-routes.ts`
  - `src/lib/axios/api-client.ts`
- Canvas/business logic should stay outside pure presentational components.

## State Management Rules

- Use **Zustand** for shared interactive canvas state (selected node, graph updates, panel state, dirty flags).
- Use **React Query** for server data lifecycle (fetching/caching/retries/invalidation).
- Keep local component state only for transient UI details (drawer toggles, form field temp values).

## Key Directories

```text
src/
  components/
    canvas/
      DataFlowCanvas.tsx
      nodes/
      panels/
      interactions/
      sidebar/
  pipeline-engine/
    graph.ts
    schema.ts
    validator.ts
    compiler.ts
    propagate.ts
  hooks/
    useMigration.ts
    useConnections.ts
    useSchemaDrift.ts
  lib/axios/
    api-client.ts
  constants/
    server-routes.ts
  store/
    canvasStore.ts
    authStore.ts
  pages/
    CanvasPage.tsx
    DashboardPage.tsx
    JobsPage.tsx
```

## Running the Frontend

```bash
npm install
npm run dev
```

Other useful scripts:

- `npm run build`
- `npm run preview`
- `npm run type-check`
- `npm run lint`
- `npm run check-all`

## Environment

Use a `.env` file in `frontend/`:

```env
VITE_API_BASE_URL=http://localhost:8000
VITE_WS_BASE_URL=ws://localhost:8004
```

## Notes for Contributors

- Keep naming aligned with backend terms: `business_name`, `technical_name`, `db_name`.
- Prefer adding behavior in reusable hooks/services before adding one-off component calls.
- For canvas features, update both:
  - rendering behavior (`components/canvas/*`)
  - data/model behavior (`pipeline-engine/*`, `store/canvasStore.ts`)

