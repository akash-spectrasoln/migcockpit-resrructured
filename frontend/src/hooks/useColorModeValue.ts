/**
 * useColorModeValue hook for Chakra UI v3 compatibility
 * In v3, useColorModeValue might not be available, so we create our own
 */
import { useColorMode } from '@chakra-ui/react'

export const useColorModeValue = <T,>(lightValue: T, darkValue: T): T => {
  const { colorMode } = useColorMode()
  return colorMode === 'light' ? lightValue : darkValue
}

