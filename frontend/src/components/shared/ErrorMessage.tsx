/**
 * Error Message Component
 * Reusable error display with optional retry action
 */
import React from 'react'
import {
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  Box,
  Button,
  VStack,
} from '@chakra-ui/react'
import { RefreshCw, AlertCircle } from 'lucide-react'

interface ErrorMessageProps {
  title?: string
  message: string
  onRetry?: () => void
  retryLabel?: string
  status?: 'error' | 'warning' | 'info'
}

export const ErrorMessage: React.FC<ErrorMessageProps> = ({
  title = 'Error',
  message,
  onRetry,
  retryLabel = 'Try Again',
  status = 'error',
}) => {
  return (
    <Alert
      status={status}
      variant="subtle"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      textAlign="center"
      borderRadius="lg"
      p={6}
    >
      <AlertIcon boxSize="40px" mr={0} as={AlertCircle} />
      <AlertTitle mt={4} mb={1} fontSize="lg">
        {title}
      </AlertTitle>
      <AlertDescription maxWidth="sm">
        <VStack spacing={4}>
          <Box>{message}</Box>
          {onRetry && (
            <Button
              leftIcon={<RefreshCw size={16} />}
              onClick={onRetry}
              size="sm"
              colorScheme={status === 'error' ? 'red' : status === 'warning' ? 'orange' : 'blue'}
            >
              {retryLabel}
            </Button>
          )}
        </VStack>
      </AlertDescription>
    </Alert>
  )
}

export default ErrorMessage

