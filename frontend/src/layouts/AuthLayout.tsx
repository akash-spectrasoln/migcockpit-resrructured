/**
 * Auth Layout
 * Layout wrapper for authentication pages (login, register, etc.)
 * Provides a centered container with background styling
 */
import React from 'react'
import { Outlet } from 'react-router-dom'
import { Box } from '@chakra-ui/react'
import { useColorModeValue } from '../hooks/useColorModeValue'

interface AuthLayoutProps {
  children?: React.ReactNode
}

export const AuthLayout: React.FC<AuthLayoutProps> = ({ children }) => {
  const bgGradient = useColorModeValue(
    'linear(to-br, blue.50, indigo.100, purple.50)',
    'linear(to-br, gray.900, blue.900, purple.900)'
  )

  return (
    <Box
      minH="100vh"
      bgGradient={bgGradient}
      display="flex"
      alignItems="center"
      justifyContent="center"
    >
      {children || <Outlet />}
    </Box>
  )
}

export default AuthLayout

