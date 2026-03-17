/**
 * Auth Context
 * Provides authentication context that wraps the existing Zustand store
 * This allows components to use either the context or the store directly
 */
import React, { createContext, useEffect, useState } from 'react'
import { useAuthStore } from '../../store/authStore'
import { StorageKeys } from '../../constants/common'

interface User {
  id?: number
  email?: string
  name?: string
  [key: string]: any
}

interface AuthState {
  isAuthenticated: boolean
  isLoading: boolean
  user: User | null
  token: string | null
}

interface AuthContextType extends AuthState {
  login: (email: string, password: string) => Promise<void>
  logout: () => void
  checkAuth: () => Promise<void>
}

export const AuthContext = createContext<AuthContextType | null>(null)

interface AuthProviderProps {
  children: React.ReactNode
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [isLoading, setIsLoading] = useState(true)
  
  // Get state and actions from Zustand store
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const token = useAuthStore((state) => state.token)
  const user = useAuthStore((state) => state.user)
  const storeLogin = useAuthStore((state) => state.login)
  const storeLogout = useAuthStore((state) => state.logout)
  const storeCheckAuth = useAuthStore((state) => state.checkAuth)

  // Check auth on mount
  useEffect(() => {
    const initAuth = async () => {
      const localStorageAuth = localStorage.getItem(StorageKeys.IS_AUTHENTICATED) === 'true'
      
      if (localStorageAuth) {
        try {
          await storeCheckAuth()
        } catch (error) {
          console.error('Auth check failed:', error)
        }
      }
      setIsLoading(false)
    }

    initAuth()
  }, [storeCheckAuth])

  const login = async (email: string, password: string) => {
    setIsLoading(true)
    try {
      await storeLogin(email, password)
    } finally {
      setIsLoading(false)
    }
  }

  const logout = () => {
    storeLogout()
  }

  const checkAuth = async () => {
    setIsLoading(true)
    try {
      await storeCheckAuth()
    } finally {
      setIsLoading(false)
    }
  }

  const value: AuthContextType = {
    isAuthenticated,
    isLoading,
    user,
    token,
    login,
    logout,
    checkAuth,
  }

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  )
}

export default AuthProvider

