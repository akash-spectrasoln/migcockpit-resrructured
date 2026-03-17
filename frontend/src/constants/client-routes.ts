/**
 * Client Routes
 * Frontend route path constants
 */

export const ClientRoutes = {
  // Auth routes
  auth: {
    login: '/login',
    register: '/register',
    forgotPassword: '/forgot-password',
    resetPassword: '/reset-password',
  },

  // Dashboard routes
  dashboard: {
    root: '/dashboard',
    projects: '/projects',
    projectNew: '/projects/new',
    projectDashboard: (projectId: string | number) => `/projects/${projectId}/dashboard`,
    canvas: '/canvas',
    jobs: '/jobs',
  },

  // Root
  root: '/',
} as const

export type ClientRoutesType = typeof ClientRoutes

