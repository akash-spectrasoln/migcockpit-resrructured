/**
 * Editable Node Name Component
 * Allows inline editing of node names directly on canvas
 */
import React, { useState, useRef, useEffect } from 'react'
import { Input, Text, Box } from '@chakra-ui/react'
import { useColorModeValue } from '../../../hooks/useColorModeValue'

interface EditableNodeNameProps {
  value: string
  onChange: (newValue: string) => void
  fontSize?: string
  fontWeight?: string
  color?: string
  isTruncated?: boolean
  flex?: number
  maxW?: string
}

export const EditableNodeName: React.FC<EditableNodeNameProps> = ({
  value,
  onChange,
  fontSize = 'xs',
  fontWeight = 'semibold',
  color,
  isTruncated = true,
  flex = 1,
  maxW,
}) => {
  const [isEditing, setIsEditing] = useState(false)
  const [editValue, setEditValue] = useState(value)
  const inputRef = useRef<HTMLInputElement>(null)
  const textColor = useColorModeValue('gray.800', 'white')

  useEffect(() => {
    setEditValue(value)
  }, [value])

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus()
      inputRef.current.select()
    }
  }, [isEditing])

  const handleDoubleClick = (e: React.MouseEvent) => {
    e.stopPropagation()
    setIsEditing(true)
  }

  const handleBlur = () => {
    if (editValue.trim() && editValue !== value) {
      onChange(editValue.trim())
    } else {
      setEditValue(value)
    }
    setIsEditing(false)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleBlur()
    } else if (e.key === 'Escape') {
      setEditValue(value)
      setIsEditing(false)
    }
  }

  if (isEditing) {
    return (
      <Input
        ref={inputRef}
        value={editValue}
        onChange={(e) => setEditValue(e.target.value)}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
        size="xs"
        fontSize={fontSize}
        fontWeight={fontWeight}
        color={color || textColor}
        flex={flex}
        maxW={maxW}
        minW="60px"
        px={1}
        py={0}
        h="auto"
        border="1px solid"
        borderColor="blue.400"
        borderRadius="sm"
        _focus={{
          borderColor: 'blue.500',
          boxShadow: '0 0 0 1px var(--chakra-colors-blue-500)',
        }}
      />
    )
  }

  return (
    <Text
      fontSize={fontSize}
      fontWeight={fontWeight}
      color={color || textColor}
      isTruncated={isTruncated}
      flex={flex}
      maxW={maxW}
      onDoubleClick={handleDoubleClick}
      cursor="pointer"
      title="Double-click to edit name"
      _hover={{
        textDecoration: 'underline',
        opacity: 0.8,
      }}
    >
      {value || 'Unnamed'}
    </Text>
  )
}

