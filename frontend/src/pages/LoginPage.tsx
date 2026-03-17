/**
 * Login Page - Chakra UI Version
 * Professional login page with Chakra UI components
 */
import React, { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Box,
  Container,
  VStack,
  Heading,
  Text,
  FormControl,
  FormLabel,
  Input,
  InputGroup,
  InputLeftElement,
  Button,
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  Icon,
  Flex,
  Divider,
} from '@chakra-ui/react'
import { useColorModeValue } from '../hooks/useColorModeValue'
import { motion } from 'framer-motion'
import { Mail, Lock, LogIn, Database, ArrowRight, AlertCircle } from 'lucide-react'
import { useAuthStore } from '../store/authStore'

const MotionBox = motion.create(Box)

export const LoginPage: React.FC = () => {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()
  const login = useAuthStore((state) => state.login)

  const bgGradient = useColorModeValue(
    'linear(to-br, blue.50, indigo.100, purple.50)',
    'linear(to-br, gray.900, blue.900, purple.900)'
  )
  const cardBg = useColorModeValue('white', 'gray.800')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    setLoading(true)

    // Normalize email (trim and lowercase)
    const normalizedEmail = email.trim().toLowerCase()
    
    // Basic email validation
    if (!normalizedEmail || !normalizedEmail.includes('@')) {
      setError('Please enter a valid email address')
      setLoading(false)
      return
    }

    try {
      console.log('Attempting login with email:', normalizedEmail)
      await login(normalizedEmail, password)
      
      // After successful login, go to dashboard
      console.log('Login successful, navigating to dashboard...')
      window.location.href = '/dashboard'
    } catch (err: any) {
      console.error('Login error:', err)
      const errorMessage = err.response?.data?.error || 
                         err.response?.data?.detail || 
                         err.message || 
                         'Login failed. Please check your credentials.'
      setError(errorMessage)
      setLoading(false)
    }
  }

  return (
    <Box
      minH="100vh"
      bgGradient={bgGradient}
      display="flex"
      alignItems="center"
      justifyContent="center"
      position="relative"
      overflow="hidden"
    >
      {/* Animated Background Elements */}
      <Box position="absolute" inset={0} overflow="hidden" pointerEvents="none">
        <MotionBox
          position="absolute"
          top="-10%"
          right="-10%"
          w="400px"
          h="400px"
          bg="blue.400"
          borderRadius="full"
          opacity={0.1}
          filter="blur(80px)"
          animate={{
            x: [0, 100, 0],
            y: [0, -100, 0],
            scale: [1, 1.2, 1],
          }}
          transition={{
            duration: 20,
            repeat: Infinity,
            ease: 'easeInOut',
          }}
        />
        <MotionBox
          position="absolute"
          bottom="-10%"
          left="-10%"
          w="400px"
          h="400px"
          bg="purple.400"
          borderRadius="full"
          opacity={0.1}
          filter="blur(80px)"
          animate={{
            x: [0, -100, 0],
            y: [0, 100, 0],
            scale: [1, 1.2, 1],
          }}
          transition={{
            duration: 20,
            repeat: Infinity,
            ease: 'easeInOut',
            delay: 0.5,
          }}
        />
      </Box>

      <Container maxW="md" position="relative" zIndex={1}>
        <MotionBox
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
        >
          <VStack spacing={8}>
            {/* Logo and Header */}
            <VStack spacing={4} textAlign="center">
              <MotionBox
                initial={{ scale: 0 }}
                animate={{ scale: 1 }}
                transition={{ delay: 0.2, type: 'spring' }}
              >
                <Flex
                  w="80px"
                  h="80px"
                  bgGradient="linear(to-br, blue.500, indigo.600)"
                  borderRadius="2xl"
                  alignItems="center"
                  justifyContent="center"
                  boxShadow="2xl"
                  _hover={{ transform: 'scale(1.05)' }}
                  transition="transform 0.2s"
                >
                  <Icon as={Database} w={10} h={10} color="white" />
                </Flex>
              </MotionBox>
              <VStack spacing={2}>
                <Heading size="2xl" color={textColor} fontWeight="bold">
                  Migration Cockpit
                </Heading>
                <Text color={subtextColor} fontSize="sm">
                  Data Migration & Transformation Platform
                </Text>
              </VStack>
            </VStack>

            {/* Login Card */}
            <Box
              w="100%"
              bg={cardBg}
              p={8}
              borderRadius="2xl"
              boxShadow="2xl"
              borderWidth="1px"
              borderColor={useColorModeValue('gray.200', 'gray.700')}
            >
              <VStack spacing={6} align="stretch">
                <VStack spacing={2} align="flex-start">
                  <Heading size="lg" color={textColor}>
                    Welcome Back
                  </Heading>
                  <Text color={subtextColor} fontSize="sm">
                    Sign in to continue to your workspace
                  </Text>
                </VStack>

                <Divider />

                <form onSubmit={handleSubmit}>
                  <VStack spacing={5}>
                    {/* Email Field */}
                    <FormControl isRequired>
                      <FormLabel color={textColor} fontSize="sm" fontWeight="medium">
                        Email Address
                      </FormLabel>
                      <InputGroup>
                        <InputLeftElement pointerEvents="none">
                          <Icon as={Mail} color="gray.400" />
                        </InputLeftElement>
                        <Input
                          type="email"
                          placeholder="you@example.com"
                          value={email}
                          onChange={(e) => setEmail(e.target.value)}
                          size="lg"
                          focusBorderColor="brand.500"
                        />
                      </InputGroup>
                    </FormControl>

                    {/* Password Field */}
                    <FormControl isRequired>
                      <FormLabel color={textColor} fontSize="sm" fontWeight="medium">
                        Password
                      </FormLabel>
                      <InputGroup>
                        <InputLeftElement pointerEvents="none">
                          <Icon as={Lock} color="gray.400" />
                        </InputLeftElement>
                        <Input
                          type="password"
                          placeholder="Enter your password"
                          value={password}
                          onChange={(e) => setPassword(e.target.value)}
                          size="lg"
                          focusBorderColor="brand.500"
                        />
                      </InputGroup>
                    </FormControl>

                    {/* Error Alert */}
                    {error && (
                      <MotionBox
                        w="100%"
                        initial={{ opacity: 0, y: -10 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.3 }}
                      >
                        <Alert status="error" borderRadius="lg">
                          <AlertIcon as={AlertCircle} />
                          <Box flex="1">
                            <AlertTitle>Login Failed</AlertTitle>
                            <AlertDescription fontSize="sm">{error}</AlertDescription>
                          </Box>
                        </Alert>
                      </MotionBox>
                    )}

                    {/* Submit Button */}
                    <Button
                      type="submit"
                      colorScheme="brand"
                      size="lg"
                      w="100%"
                      isLoading={loading}
                      loadingText="Signing in..."
                      rightIcon={<ArrowRight />}
                      leftIcon={<LogIn />}
                      variant="canvas-action"
                    >
                      Sign In
                    </Button>
                  </VStack>
                </form>

                <Divider />

                {/* Footer */}
                <Text fontSize="xs" color={subtextColor} textAlign="center">
                  Secure access to your migration workspace
                </Text>
              </VStack>
            </Box>

            {/* Help Text */}
            <Text fontSize="xs" color={subtextColor} textAlign="center">
              Need help? Contact your administrator
            </Text>
          </VStack>
        </MotionBox>
      </Container>
    </Box>
  )
}

