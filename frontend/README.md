# Frontend - Data Migration Cockpit

React-based frontend application for the Data Migration Cockpit, built with modern web technologies.

## Technology Stack

- **React 18** - UI library
- **TypeScript** - Type safety
- **Chakra UI** - Component library and theming
- **React Flow** - Canvas/diagram functionality
- **React Query** - Data fetching and caching
- **Zustand** - State management
- **Vite** - Build tool and dev server
- **Framer Motion** - Animations
- **Axios** - HTTP client
- **Socket.IO Client** - WebSocket communication

## Project Structure

```
frontend/
├── src/
│   ├── components/          # React components
│   │   └── Canvas/          # Canvas-related components
│   │       ├── DataFlowCanvasChakra.tsx      # Main canvas (Chakra UI)
│   │       ├── NodePaletteChakra.tsx         # Node palette sidebar
│   │       ├── NodeConfigPanelChakra.tsx     # Configuration drawer
│   │       ├── NodeTypesChakra.tsx            # Custom node types
│   │       └── EdgeTypes.tsx                 # Custom edge types
│   │
│   ├── pages/               # Page components
│   │   ├── LoginPageChakra.tsx    # Login page
│   │   ├── CanvasPageChakra.tsx   # Canvas page
│   │   ├── JobsPage.tsx            # Jobs monitoring page
│   │   ├── LoginPage.tsx           # Re-exports Chakra version
│   │   └── CanvasPage.tsx          # Re-exports Chakra version
│   │
│   ├── hooks/               # React Query hooks
│   │   ├── useCanvas.ts     # Canvas operations
│   │   ├── useMigration.ts  # Migration operations
│   │   └── useConnections.ts # Connection operations
│   │
│   ├── services/            # API and WebSocket services
│   │   ├── api.ts           # Axios API client
│   │   └── websocket.ts     # WebSocket client
│   │
│   ├── store/               # Zustand stores
│   │   ├── authStore.ts     # Authentication state
│   │   └── canvasStore.ts   # Canvas state
│   │
│   ├── providers/           # React context providers
│   │   └── AppProviders.tsx # Chakra UI + React Query providers
│   │
│   ├── theme/               # Theme configuration
│   │   └── theme.ts         # Chakra UI theme
│   │
│   ├── types/               # TypeScript types
│   │   └── nodeRegistry.ts  # Node type definitions
│   │
│   ├── App.tsx              # Main app component
│   ├── main.tsx             # Entry point
│   └── index.css            # Global styles
│
├── index.html               # HTML template
├── package.json             # Dependencies
├── vite.config.ts          # Vite configuration
├── tsconfig.json           # TypeScript configuration
└── tailwind.config.js      # Tailwind CSS configuration (legacy)
```

## Getting Started

### Prerequisites

- Node.js 16+ and npm

### Installation

```bash
npm install
```

### Development

Start the development server:

```bash
npm run dev
```

The app will be available at http://localhost:3000

### Build

Build for production:

```bash
npm run build
```

The built files will be in the `dist/` directory.

### Preview Production Build

```bash
npm run preview
```

## Key Features

### Canvas Interface
- Drag-and-drop node creation
- Visual pipeline building
- Real-time validation
- Node configuration panels
- Multiple view modes (Design, Validate, Monitor)

### State Management
- **Zustand** for global state (auth, canvas)
- **React Query** for server state (API data)
- Local component state for UI interactions

### Styling
- **Chakra UI** for all components
- Custom theme with brand colors
- Responsive design
- Dark mode support (prepared)

### Data Fetching
- React Query hooks for automatic caching
- Optimistic updates
- Background refetching
- Error handling

## Component Architecture

### Pages
- **LoginPage** - Authentication
- **CanvasPage** - Main canvas interface
- **JobsPage** - Migration job monitoring

### Canvas Components
- **DataFlowCanvasChakra** - Main canvas orchestrator
- **NodePaletteChakra** - Left sidebar with draggable nodes
- **NodeConfigPanelChakra** - Right drawer for node configuration
- **NodeTypesChakra** - Custom React Flow node components

### Hooks
- **useCanvas** - Canvas CRUD operations
- **useMigration** - Migration job management
- **useConnections** - Connection and metadata fetching

## API Integration

The frontend communicates with:
- **Django REST API** (port 8000) - Main API
- **FastAPI Services** (ports 8001-8004) - Microservices
- **WebSocket Server** (port 8004) - Real-time updates

## Environment Variables

Create a `.env` file in the frontend directory:

```env
VITE_API_BASE_URL=http://localhost:8000
VITE_WS_BASE_URL=ws://localhost:8004
```

## Development Notes

### Adding New Components
1. Create component in appropriate directory
2. Use Chakra UI components
3. Follow TypeScript best practices
4. Add to appropriate exports

### Adding New Hooks
1. Create hook in `hooks/` directory
2. Use React Query for API calls
3. Export from hook file

### Styling Guidelines
- Use Chakra UI components and props
- Use theme tokens for colors
- Follow responsive design patterns
- Use framer-motion for animations

## Troubleshooting

### Port Already in Use
Change the port in `vite.config.ts`:
```typescript
server: {
  port: 3001, // Change to available port
}
```

### API Connection Issues
- Verify Django server is running on port 8000
- Check CORS settings in Django
- Verify API_BASE_URL in environment

### Build Errors
- Clear `node_modules` and reinstall: `rm -rf node_modules && npm install`
- Clear Vite cache: `rm -rf node_modules/.vite`

## Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build
- `npm run lint` - Run ESLint

## Dependencies

See `package.json` for complete list of dependencies.

### Key Dependencies
- `@chakra-ui/react` - UI component library
- `reactflow` - Canvas/diagram library
- `@tanstack/react-query` - Data fetching
- `zustand` - State management
- `axios` - HTTP client
- Native `WebSocket` - real-time progress communication

