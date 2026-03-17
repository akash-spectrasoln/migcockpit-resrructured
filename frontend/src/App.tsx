/**
 * App Component
 * Main application entry point using the centralized router
 */
import React from 'react'
import { RouterProvider } from 'react-router-dom'
import { router } from './routes/AppRoutes'

function App() {
  return <RouterProvider router={router} />
}

export default App
