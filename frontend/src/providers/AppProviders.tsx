/**
 * App Providers
 * Wraps the app with Chakra UI, React Query, Auth, and other providers
 */
import React from 'react'
import { ChakraProvider } from '@chakra-ui/react'
import { QueryClientProvider } from '@tanstack/react-query'
import { queryClient } from '../lib/react-query-client'
import { AuthProvider } from '../context/Auth/AuthContext'
import theme from '../theme/theme'

interface AppProvidersProps {
  children: React.ReactNode
}

export const AppProviders: React.FC<AppProvidersProps> = ({ children }) => {
  return (
    <ChakraProvider theme={theme}>
      <AuthProvider>
        <QueryClientProvider client={queryClient}>
          {children}
        </QueryClientProvider>
      </AuthProvider>
    </ChakraProvider>
  )
}

export default AppProviders
