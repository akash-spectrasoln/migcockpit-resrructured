/**
 * Application Routes
 * Centralized routing configuration using createBrowserRouter
 */
import React, { Suspense, lazy } from 'react'
import { createBrowserRouter, Navigate, Outlet } from 'react-router-dom'
import { ProtectedRoute } from '../components/auth/ProtectedRoute'
import { Spinner } from '../components/shared/Spinner'
import { ClientRoutes } from '../constants/client-routes'

// Lazy load pages for better performance
const LoginPage = lazy(() => import('../pages/LoginPage').then(m => ({ default: m.LoginPage })))
const DashboardPage = lazy(() => import('../pages/DashboardPage').then(m => ({ default: m.DashboardPage })))
const ProjectsListPage = lazy(() => import('../pages/ProjectsListPage').then(m => ({ default: m.ProjectsListPage })))
const CreateProjectPage = lazy(() => import('../pages/CreateProjectPage').then(m => ({ default: m.CreateProjectPage })))
const ProjectDashboardPage = lazy(() => import('../pages/ProjectDashboardPage').then(m => ({ default: m.ProjectDashboardPage })))
const CanvasPage = lazy(() => import('../pages/CanvasPage').then(m => ({ default: m.CanvasPage })))
const JobsPage = lazy(() => import('../pages/JobsPage').then(m => ({ default: m.JobsPage })))

// Suspense wrapper for lazy loaded components
const SuspenseWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <Suspense fallback={<Spinner centered fullScreen text="Loading..." />}>
    {children}
  </Suspense>
)

// Protected route wrapper
const ProtectedWrapper: React.FC = () => (
  <ProtectedRoute>
    <SuspenseWrapper>
      <Outlet />
    </SuspenseWrapper>
  </ProtectedRoute>
)

export const router = createBrowserRouter(
  [
    // Public routes
    {
      path: ClientRoutes.auth.login,
      element: (
        <SuspenseWrapper>
          <LoginPage />
        </SuspenseWrapper>
      ),
    },

    // Protected routes
    {
      element: <ProtectedWrapper />,
      children: [
        {
          path: ClientRoutes.dashboard.projects,
          element: <ProjectsListPage />,
        },
        {
          path: ClientRoutes.dashboard.projectNew,
          element: <CreateProjectPage />,
        },
        {
          path: '/projects/:projectId/dashboard',
          element: <ProjectDashboardPage />,
        },
        {
          path: ClientRoutes.dashboard.root,
          element: <DashboardPage />,
        },
        {
          path: ClientRoutes.dashboard.canvas,
          element: <CanvasPage />,
        },
        {
          path: ClientRoutes.dashboard.jobs,
          element: <JobsPage />,
        },
      ],
    },

    // Root redirect
    {
      path: ClientRoutes.root,
      element: <Navigate to={ClientRoutes.dashboard.projects} replace />,
    },

    // Catch all - redirect to projects
    {
      path: '*',
      element: <Navigate to={ClientRoutes.dashboard.projects} replace />,
    },
  ],
  {
    future: {
      v7_startTransition: true,
      v7_relativeSplatPath: true,
    } as any,
  }
)

export default router

