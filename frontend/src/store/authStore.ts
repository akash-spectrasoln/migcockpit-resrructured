import { create } from 'zustand'
import { api } from '../services/api'

interface AuthState {
  isAuthenticated: boolean
  token: string | null
  user: any | null
  login: (email: string, password: string) => Promise<void>
  logout: () => void
  checkAuth: () => Promise<void>
}

export const useAuthStore = create<AuthState>((set) => ({
  isAuthenticated: localStorage.getItem('is_authenticated') === 'true',
  token: localStorage.getItem('access_token') || 'cookie-based',
  user: null,

  login: async (email: string, password: string) => {
    try {
      // Ensure email is normalized
      const normalizedEmail = email.trim().toLowerCase()
      console.log('Login request - Email:', normalizedEmail, 'Password length:', password.length)
      
      const response = await api.post('/api/api-login/', { 
        email: normalizedEmail, 
        password: password 
      })
      
      console.log('Login response:', response.status, response.data)
      
      // Backend uses HTTP-only cookies, but we also check response for user data
      // Tokens are stored in cookies automatically by the browser
      // We'll use a flag in localStorage to track authentication
      localStorage.setItem('is_authenticated', 'true')
      
      // Store user info if available
      // IMPORTANT: Update state synchronously before returning
      if (response.data.user) {
        set({ isAuthenticated: true, user: response.data.user, token: 'cookie-based' })
      } else {
        set({ isAuthenticated: true, token: 'cookie-based' })
      }
    } catch (error: any) {
      console.error('Login failed:', error)
      console.error('Error response:', error.response?.data)
      console.error('Error status:', error.response?.status)
      localStorage.removeItem('is_authenticated')
      throw error
    }
  },

  logout: async () => {
    try {
      // Call logout endpoint to clear cookies
      await api.post('/api/api-logout/')
    } catch (error) {
      console.error('Logout error:', error)
    }
    localStorage.removeItem('access_token')
    localStorage.removeItem('refresh_token')
    localStorage.removeItem('is_authenticated')
    set({ isAuthenticated: false, token: null, user: null })
  },

  checkAuth: async () => {
    const isAuth = localStorage.getItem('is_authenticated')
    if (isAuth === 'true') {
      try {
        // Verify authentication by making a test request
        await api.get('/api/projects/')
        set({ isAuthenticated: true, token: 'cookie-based' })
      } catch (error) {
        // Authentication invalid, clear it
        localStorage.removeItem('is_authenticated')
        localStorage.removeItem('access_token')
        localStorage.removeItem('refresh_token')
        set({ isAuthenticated: false, token: null })
      }
    } else {
      set({ isAuthenticated: false, token: null })
    }
  },
}))

