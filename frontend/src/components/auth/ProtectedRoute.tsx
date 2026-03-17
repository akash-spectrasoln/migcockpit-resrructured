/**
 * Protected Route Component
 * Guards routes that require authentication
 * Redirects to login if user is not authenticated
 */
import React from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { useAuthStore } from '../../store/authStore'
import { Spinner } from '../shared/Spinner'
import { ClientRoutes } from '../../constants/client-routes'
import { StorageKeys } from '../../constants/common'

interface ProtectedRouteProps {
  children: React.ReactNode
}

export const ProtectedRoute: React.FC<ProtectedRouteProps> = ({ children }) => {
  const location = useLocation()
  
  // Check localStorage first (most reliable after login redirect)
  const localStorageAuth = localStorage.getItem(StorageKeys.IS_AUTHENTICATED) === 'true'
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const checkAuth = useAuthStore((state) => state.checkAuth)
  
  // Use localStorage as primary source, Zustand as secondary
  const authStatus = localStorageAuth || isAuthenticated
  const [isChecking, setIsChecking] = React.useState(!authStatus)

  React.useEffect(() => {
    // If not authenticated, check with backend
    if (!authStatus) {
      const verifyAuth = async () => {
        await checkAuth()
        setIsChecking(false)
      }
      verifyAuth()
    } else {
      setIsChecking(false)
    }
  }, [authStatus, checkAuth])

  // Show loading while checking auth
  if (isChecking) {
    return <Spinner centered text="Verifying authentication..." />
  }

  // Get fresh auth state after check
  const currentAuth = localStorage.getItem(StorageKeys.IS_AUTHENTICATED) === 'true' || 
                      useAuthStore.getState().isAuthenticated
  
  if (!currentAuth) {
    // Redirect to login, preserving the intended destination
    return <Navigate to={ClientRoutes.auth.login} state={{ from: location }} replace />
  }

  return <>{children}</>
}

export default ProtectedRoute

