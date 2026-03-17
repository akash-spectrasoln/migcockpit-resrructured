/**
 * Dashboard Layout
 * Layout wrapper for authenticated dashboard pages
 * Provides consistent header, sidebar (optional), and main content area
 */
import React from 'react'
import { Outlet, useNavigate } from 'react-router-dom'
import {
  Box,
  Flex,
  Heading,
  Text,
  Button,
  HStack,
  VStack,
  IconButton,
  useDisclosure,
  Drawer,
  DrawerOverlay,
  DrawerContent,
  DrawerHeader,
  DrawerBody,
  DrawerCloseButton,
} from '@chakra-ui/react'
import { useColorModeValue } from '../hooks/useColorModeValue'
import { useAuthStore } from '../store/authStore'
import { Menu, Home, FolderOpen, Database, Settings, LogOut } from 'lucide-react'
import { ViewConfig } from '../constants/common'

interface DashboardLayoutProps {
  children?: React.ReactNode
  title?: string
  subtitle?: string
  showSidebar?: boolean
  headerActions?: React.ReactNode
}

export const DashboardLayout: React.FC<DashboardLayoutProps> = ({
  children,
  title,
  subtitle,
  showSidebar = false,
  headerActions,
}) => {
  const navigate = useNavigate()
  const { logout } = useAuthStore()
  const { isOpen, onOpen, onClose } = useDisclosure()

  const bg = useColorModeValue('gray.50', 'gray.900')
  const headerBg = useColorModeValue('white', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')
  const textColor = useColorModeValue('gray.800', 'white')
  const subtextColor = useColorModeValue('gray.600', 'gray.300')

  const navItems = [
    { icon: Home, label: 'Dashboard', path: '/dashboard' },
    { icon: FolderOpen, label: 'Projects', path: '/projects' },
    { icon: Database, label: 'Canvas', path: '/canvas' },
    { icon: Settings, label: 'Jobs', path: '/jobs' },
  ]

  const handleLogout = () => {
    logout()
    navigate('/login')
  }

  const SidebarContent = () => (
    <VStack align="stretch" spacing={2} py={4}>
      {navItems.map((item) => (
        <Button
          key={item.path}
          leftIcon={<item.icon size={18} />}
          variant="ghost"
          justifyContent="flex-start"
          onClick={() => {
            navigate(item.path)
            onClose()
          }}
          px={4}
          py={2}
        >
          {item.label}
        </Button>
      ))}
      <Box pt={4} borderTopWidth="1px" borderColor={borderColor} mt={4}>
        <Button
          leftIcon={<LogOut size={18} />}
          variant="ghost"
          justifyContent="flex-start"
          colorScheme="red"
          onClick={handleLogout}
          px={4}
          py={2}
          w="100%"
        >
          Logout
        </Button>
      </Box>
    </VStack>
  )

  return (
    <Box w="100%" h="100vh" bg={bg} display="flex" flexDirection="column">
      {/* Header */}
      <Box
        px={8}
        py={4}
        borderBottomWidth="1px"
        borderColor={borderColor}
        bg={headerBg}
        h={`${ViewConfig.headerHeight}px`}
        flexShrink={0}
      >
        <Flex justify="space-between" align="center" h="100%">
          <HStack spacing={4}>
            {showSidebar && (
              <IconButton
                aria-label="Open menu"
                icon={<Menu size={20} />}
                variant="ghost"
                onClick={onOpen}
                display={{ base: 'flex', md: showSidebar ? 'flex' : 'none' }}
              />
            )}
            <VStack align="flex-start" spacing={0}>
              {title && (
                <Heading size="md" color={textColor}>
                  {title}
                </Heading>
              )}
              {subtitle && (
                <Text fontSize="sm" color={subtextColor}>
                  {subtitle}
                </Text>
              )}
            </VStack>
          </HStack>
          <HStack spacing={3}>
            {headerActions}
            <Button variant="outline" onClick={handleLogout}>
              Logout
            </Button>
          </HStack>
        </Flex>
      </Box>

      {/* Main Content */}
      <Box flex={1} overflow="auto">
        {children || <Outlet />}
      </Box>

      {/* Mobile Sidebar Drawer */}
      {showSidebar && (
        <Drawer isOpen={isOpen} placement="left" onClose={onClose}>
          <DrawerOverlay />
          <DrawerContent>
            <DrawerCloseButton />
            <DrawerHeader borderBottomWidth="1px">Navigation</DrawerHeader>
            <DrawerBody p={0}>
              <SidebarContent />
            </DrawerBody>
          </DrawerContent>
        </Drawer>
      )}
    </Box>
  )
}

export default DashboardLayout

