/**
 * Spinner Component
 * Reusable loading spinner with optional text
 */
import React from 'react'
import {
  Spinner as ChakraSpinner,
  VStack,
  Text,
  Flex,
  SpinnerProps as ChakraSpinnerProps,
} from '@chakra-ui/react'
import { useColorModeValue } from '../../hooks/useColorModeValue'

interface SpinnerProps extends ChakraSpinnerProps {
  text?: string
  centered?: boolean
  fullScreen?: boolean
}

export const Spinner: React.FC<SpinnerProps> = ({
  text,
  centered = false,
  fullScreen = false,
  size = 'lg',
  ...props
}) => {
  const textColor = useColorModeValue('gray.600', 'gray.300')

  const content = (
    <VStack spacing={4}>
      <ChakraSpinner
        size={size}
        thickness="4px"
        speed="0.65s"
        emptyColor="gray.200"
        color="brand.500"
        {...props}
      />
      {text && (
        <Text fontSize="sm" color={textColor}>
          {text}
        </Text>
      )}
    </VStack>
  )

  if (fullScreen) {
    return (
      <Flex
        position="fixed"
        top={0}
        left={0}
        right={0}
        bottom={0}
        justify="center"
        align="center"
        bg="blackAlpha.300"
        zIndex={9999}
      >
        {content}
      </Flex>
    )
  }

  if (centered) {
    return (
      <Flex justify="center" align="center" h="100%" minH="200px">
        {content}
      </Flex>
    )
  }

  return content
}

export default Spinner

