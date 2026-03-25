/**
 * Compute Node Configuration Panel
 * IDE-style code editor for custom DataFrame transformations
 */
import React, { useState, useEffect, useCallback, useRef } from 'react'
import {
    Box,
    VStack,
    HStack,
    Text,
    Button,
    Select,
    Tabs,
    TabList,
    TabPanels,
    Tab,
    TabPanel,
    Alert,
    AlertIcon,
    useColorModeValue,
    FormControl,
    FormLabel,
    Input,
    Textarea,
    Table,
    Thead,
    Tbody,
    Tr,
    Th,
    Td,
    useToast,
    Modal,
    ModalOverlay,
    ModalContent,
    ModalHeader,
    ModalBody,
    ModalCloseButton,
    useDisclosure,
    IconButton,
} from '@chakra-ui/react'
import { CheckCircle, Code, AlertCircle, Eye, Play, Maximize2, FileCheck, Save } from 'lucide-react'
import { Node, Edge } from 'reactflow'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'

interface ComputeNodeConfigPanelProps {
    node: Node | null
    nodes: Node[]
    edges: Edge[]
    onUpdate: (nodeId: string, updateData: any) => void
}

interface ColumnMetadata {
    name: string
    datatype?: string
    nullable?: boolean
}

export const ComputeNodeConfigPanel: React.FC<ComputeNodeConfigPanelProps> = ({
    node,
    nodes,
    edges,
    onUpdate,
}) => {
    const [code, setCode] = useState<string>('')
    const [requirements, setRequirements] = useState<string>('')
    const [language, setLanguage] = useState<string>('python')
    const [availableColumns, setAvailableColumns] = useState<ColumnMetadata[]>([])
    const [outputPreview, setOutputPreview] = useState<any>(null)
    const [errors, setErrors] = useState<string[]>([])
    const [loading, setLoading] = useState(false)
    const [executing, setExecuting] = useState(false)
    const [compiling, setCompiling] = useState(false)
    const [compileResult, setCompileResult] = useState<{ success: boolean; message?: string; error?: string; line_number?: number } | null>(null)
    const [tabIndex, setTabIndex] = useState(0)
    const [businessName, setBusinessName] = useState<string>('')
    const [technicalName, setTechnicalName] = useState<string>('')
    const { isOpen: isExpanded, onOpen: onExpand, onClose: onCloseExpanded } = useDisclosure()

    const toast = useToast()

    const bg = useColorModeValue('white', 'gray.800')
    const borderColor = useColorModeValue('gray.200', 'gray.700')
    const headerBg = useColorModeValue('gray.50', 'gray.700')
    const codeBg = useColorModeValue('gray.50', 'gray.900')
    const codeTheme = useColorModeValue(undefined, oneDark)

    const previousNodeId = React.useRef<string | null>(null)
    const liveSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const lastPushedConfigRef = useRef<string>('')

    /**
     * Normalize code to canonical form by removing comments, example code, and malformed text.
     * If user hasn't added custom logic, converts to: _output_df = _input_df.copy()
     */
    const normalizeCode = useCallback((code: string): string => {
        // PRESERVE ALL WHITESPACE - no trimming, no normalization
        // Return code exactly as entered
        if (!code) {
            return '_output_df = _input_df.copy()'
        }
        
        // Only check if completely empty (after trimming check), but preserve original
        if (!code.trim()) {
            return '_output_df = _input_df.copy()'
        }
        
        // Return code exactly as-is - preserve all whitespace, indentation, empty lines, trailing spaces
        return code
    }, [])

    useEffect(() => {
        if (!node) return

        if (previousNodeId.current !== node.id) {
            const config = node.data.config || {}
            // Default template for new Compute nodes
            const defaultTemplate = `# Input DataFrame: _input_df (read-only, contains data from upstream node)
# Output DataFrame: _output_df (required, assign your result here)

_output_df = _input_df.copy()`

            setCode(config.code || defaultTemplate)
            setRequirements(config.requirements || '')
            setLanguage(config.language || 'python')
            setBusinessName(node.data.business_name || node.data.label || '')
            setTechnicalName(node.data.technical_name || node.id || '')
            setErrors([])
            setOutputPreview(null)
            previousNodeId.current = node.id

            const inputNodeIds = node.data.input_nodes || []
            if (inputNodeIds.length > 0 && nodes) {
                const inputNode = nodes.find((n) => n.id === inputNodeIds[0])
                if (inputNode) {
                    loadColumns(inputNode)
                }
            } else if (edges && nodes) {
                const inputEdge = edges.find((e) => e.target === node.id)
                if (inputEdge) {
                    const inputNode = nodes.find((n) => n.id === inputEdge.source)
                    if (inputNode) {
                        loadColumns(inputNode)
                    }
                }
            }
        }
    }, [node?.id, nodes, edges])

    const loadColumns = async (inputNode: Node): Promise<void> => {
        setLoading(true)

        try {
            // ✅ SPECIAL HANDLING FOR JOIN NODES: Use outputColumns with resolved outputName
            if (inputNode.data.type === 'join' && inputNode.data.config?.outputColumns) {
                const outputColumns = inputNode.data.config.outputColumns
                const includedColumns = outputColumns.filter((col: any) => col.included !== false)
                
                if (includedColumns.length > 0) {
                    const columnMetadata: ColumnMetadata[] = includedColumns.map((col: any) => {
                        const outputName = col.outputName || col.column
                        return {
                            name: outputName, // Use resolved outputName (e.g., "src_config_id_l")
                            datatype: col.datatype || col.data_type || col.type || 'TEXT',
                            nullable: col.nullable !== undefined ? col.nullable : true,
                        }
                    })
                    
                    if (columnMetadata.length > 0) {
                        console.log('[ComputeNodeConfig] Using columns from join outputColumns (with resolved names):', columnMetadata)
                        setAvailableColumns(columnMetadata)
                        setLoading(false)
                        return
                    }
                }
            }
            
            if (inputNode.data.type === 'source' && inputNode.data.config) {
                const config = inputNode.data.config
                if (config.sourceId && config.tableName) {
                    const { sourceTableApi } = await import('../../../services/api')
                    const response = await sourceTableApi.getColumns(
                        config.sourceId,
                        config.tableName,
                        config.schema
                    )
                    const columns = response.data.columns || []
                    const columnMetadata: ColumnMetadata[] = columns.map((col: any) => ({
                        name: col.name || col.column_name,
                        datatype: col.data_type || col.datatype || 'TEXT',
                        nullable: col.nullable !== undefined ? col.nullable : true,
                    }))
                    setAvailableColumns(columnMetadata)
                    setLoading(false)
                    return
                }
            }

            if (inputNode.data.output_metadata && inputNode.data.output_metadata.columns) {
                const columns = inputNode.data.output_metadata.columns.map((col: any) => ({
                    name: typeof col === 'string' ? col : (col.name || col.column_name || col),
                    datatype: typeof col === 'string' ? 'TEXT' : (col.datatype || col.data_type || col.type || 'TEXT'),
                    nullable: typeof col === 'string' ? true : (col.nullable !== undefined ? col.nullable : true),
                }))
                setAvailableColumns(columns)
                setLoading(false)
                return
            }

            if (inputNode.data.config?.columns) {
                const columns = Array.isArray(inputNode.data.config.columns)
                    ? inputNode.data.config.columns.map((col: any) => ({
                        name: typeof col === 'string' ? col : (col.name || col.column_name || col),
                        datatype: typeof col === 'string' ? 'TEXT' : (col.datatype || col.data_type || col.type || 'TEXT'),
                        nullable: typeof col === 'string' ? true : (col.nullable !== undefined ? col.nullable : true),
                    }))
                    : []
                setAvailableColumns(columns)
                setLoading(false)
                return
            }

            setErrors(['Could not determine available columns from input node'])
            setLoading(false)
        } catch (err: any) {
            console.error('[ComputeNodeConfig] Error loading columns:', err)
            setErrors([err.message || 'Failed to load columns'])
            setLoading(false)
        } finally {
            setLoading(false)
        }
    }

    const handleSave = useCallback(() => {
        if (!node) return

        if (!code.trim()) {
            setErrors(['Code cannot be empty'])
            return
        }

        // Save code exactly as entered - no normalization/trimming
        const config = {
            ...(node.data?.config || {}),
            code: code, // Preserve code exactly as entered
            requirements: requirements,
            language: language,
        }

        onUpdate(node.id, {
            config: config,
            business_name: businessName || 'Compute',
            technical_name: technicalName || node.id,
            output_metadata: null,
        })

        toast({
            title: 'Compute Node Saved',
            description: 'Code and dependencies have been saved successfully.',
            status: 'success',
            duration: 3000,
            isClosable: true,
        })

        setErrors([])
    }, [node, code, requirements, language, onUpdate, businessName, technicalName, toast])

    // Live updates (debounced): avoid updating canvas store on every keystroke.
    useEffect(() => {
        if (!node || !code.trim()) return

        const config = {
            ...(node.data?.config || {}),
            code,
            requirements,
            language,
        }
        const payload = {
            config,
            business_name: businessName || 'Compute',
            technical_name: technicalName || node.id,
            output_metadata: null,
        }
        const signature = JSON.stringify(payload)
        if (signature === lastPushedConfigRef.current) return

        if (liveSaveTimerRef.current) {
            clearTimeout(liveSaveTimerRef.current)
        }
        liveSaveTimerRef.current = setTimeout(() => {
            onUpdate(node.id, payload)
            lastPushedConfigRef.current = signature
            liveSaveTimerRef.current = null
        }, 350)

        return () => {
            if (liveSaveTimerRef.current) {
                clearTimeout(liveSaveTimerRef.current)
                liveSaveTimerRef.current = null
            }
        }
    }, [node?.id, code, requirements, language, businessName, technicalName, onUpdate])

    const handleRun = useCallback(async () => {
        if (!node) return
        if (!code.trim()) {
            setErrors(['Code cannot be empty'])
            setTabIndex(2) // Switch to Errors tab
            return
        }

        // Use code exactly as entered - preserve all whitespace
        const finalCode = code

        setExecuting(true)
        setErrors([])
        setOutputPreview(null)

        try {
            const { pipelineApi } = await import('../../../services/api')

            // Construct a temporary node with the normalized code to send to backend
            // This ensures we execute clean code, not template with comments
            const tempNode = {
                ...node,
                data: {
                    ...node.data,
                    config: {
                        ...(node.data.config || {}),
                        code: finalCode, // Use code exactly as entered
                        requirements: requirements, // Use current requirements
                    }
                }
            }

            // Replace the current node in the nodes array
            const executionNodes = nodes.map(n => n.id === node.id ? tempNode : n)

            const response = await pipelineApi.execute(
                executionNodes,
                edges,
                node.id,
                { page: 1, pageSize: 100, forceRefresh: true }
            )

            const { rows, columns } = response.data

            setOutputPreview({
                rows,
                columns
            })
            setTabIndex(1) // Switch to Output tab

            toast({
                title: 'Execution Successful',
                description: `Processed ${rows.length} rows.`,
                status: 'success',
                duration: 3000,
                isClosable: true,
            })

        } catch (err: any) {
            console.error('[ComputeNodeConfig] Execution error:', err)
            const errorMessage = err.response?.data?.error || err.response?.data?.details || err.message || 'Execution failed'
            setErrors([errorMessage])
            setTabIndex(2) // Switch to Errors tab

            toast({
                title: 'Execution Failed',
                description: 'Check the Errors tab for details.',
                status: 'error',
                duration: 5000,
                isClosable: true,
            })
        } finally {
            setExecuting(false)
        }
    }, [node, code, requirements, nodes, edges, toast])

    const handleCompile = useCallback(async () => {
        if (!node) return
        if (!code.trim()) {
            setErrors(['Code cannot be empty'])
            setTabIndex(2) // Switch to Errors tab
            return
        }

        // Use code exactly as entered - preserve all whitespace
        const finalCode = code

        setCompiling(true)
        setCompileResult(null)
        setErrors([])

        try {
            const { api } = await import('../../../services/api')
            
            const response = await api.post('/api/compute/compile/', {
                code: finalCode, // Use code exactly as entered
                language: language,
                normalize: false // Don't normalize - preserve all whitespace
            })

            if (response.data.success) {
                setCompileResult({
                    success: true,
                    message: response.data.message || 'Compilation successful'
                })
                toast({
                    title: 'Compilation Successful',
                    description: response.data.message || 'Code is valid and ready to execute.',
                    status: 'success',
                    duration: 3000,
                    isClosable: true,
                })
            } else {
                const errorMsg = response.data.error || 'Compilation failed'
                const lineNum = response.data.line_number
                const errorType = response.data.error_type || 'Error'
                
                setCompileResult({
                    success: false,
                    error: errorMsg,
                    line_number: lineNum
                })
                setErrors([`[${errorType}] ${errorMsg}${lineNum ? ` (Line ${lineNum})` : ''}`])
                setTabIndex(2) // Switch to Errors tab
                
                toast({
                    title: 'Compilation Failed',
                    description: errorMsg + (lineNum ? ` (Line ${lineNum})` : ''),
                    status: 'error',
                    duration: 5000,
                    isClosable: true,
                })
            }
        } catch (err: any) {
            console.error('[ComputeNodeConfig] Compilation error:', err)
            const errorMessage = err.response?.data?.error || err.response?.data?.details || err.message || 'Compilation failed'
            const lineNum = err.response?.data?.line_number
            const errorType = err.response?.data?.error_type || 'Error'
            
            setCompileResult({
                success: false,
                error: errorMessage,
                line_number: lineNum
            })
            setErrors([`[${errorType}] ${errorMessage}${lineNum ? ` (Line ${lineNum})` : ''}`])
            setTabIndex(2) // Switch to Errors tab

            toast({
                title: 'Compilation Failed',
                description: errorMessage + (lineNum ? ` (Line ${lineNum})` : ''),
                status: 'error',
                duration: 5000,
                isClosable: true,
            })
        } finally {
            setCompiling(false)
        }
        // Update local code state to normalized version
        setCode(finalCode)
    }, [node, code, language, toast])

    return (
        <>
            <Box bg={bg} h="100%" display="flex" flexDirection="column">
                <Box bg={headerBg} p={4} borderBottomWidth="1px" borderColor={borderColor}>
                    <HStack justify="space-between" align="center" mb={2}>
                        <Text fontSize="lg" fontWeight="semibold">
                            Compute Node
                        </Text>
                        <HStack>
                            <IconButton
                                aria-label="Expand editor"
                                icon={<Maximize2 size={16} />}
                                size="sm"
                                variant="ghost"
                                onClick={onExpand}
                                title="Expand editor"
                            />
                            <Button
                                leftIcon={<FileCheck size={16} />}
                                size="sm"
                                colorScheme="purple"
                                variant="outline"
                                onClick={handleCompile}
                                isLoading={compiling}
                                loadingText="Compiling"
                                isDisabled={loading || executing || !code.trim()}
                                title="Validate code syntax and contracts without executing"
                            >
                                Compile
                            </Button>
                            <Button
                                leftIcon={<Play size={16} />}
                                size="sm"
                                colorScheme="blue"
                                onClick={handleRun}
                                isLoading={executing}
                                loadingText="Running"
                                isDisabled={loading || !code.trim()}
                            >
                                Run
                            </Button>
                            {/* Live updates: no per-node Save button */}
                        </HStack>
                    </HStack>
                    {compileResult && (
                        <Alert
                            status={compileResult.success ? 'success' : 'error'}
                            size="sm"
                            borderRadius="md"
                            mt={2}
                        >
                            <AlertIcon />
                            <Text fontSize="xs">
                                {compileResult.success 
                                    ? compileResult.message || 'Compilation successful'
                                    : compileResult.error || 'Compilation failed'
                                }
                                {compileResult.line_number && !compileResult.success && ` (Line ${compileResult.line_number})`}
                            </Text>
                        </Alert>
                    )}
                    {/* Schema-drift error banner */}
                    {(() => {
                        const schemaDriftErrors: any[] = Array.isArray((node as any)?.data?.config_errors)
                            ? (node as any).data.config_errors.filter((e: any) => e.source === 'schema_drift')
                            : []
                        const flatSchemaErrors: string[] = Array.isArray((node as any)?.data?.errors)
                            ? (node as any).data.errors.filter((e: string) => e.includes('not found'))
                            : []
                        if (schemaDriftErrors.length === 0 && flatSchemaErrors.length === 0) return null
                        return (
                            <Alert status="error" size="sm" borderRadius="md" mt={2}>
                                <AlertIcon />
                                <Box w="100%">
                                    <Text fontSize="xs" fontWeight="semibold" mb={1}>
                                        ❌ Schema errors — columns removed upstream
                                    </Text>
                                    {schemaDriftErrors.length > 0
                                        ? schemaDriftErrors.map((e: any, i: number) => (
                                            <Text key={i} fontSize="xs" mt={0.5}>• {e.message}</Text>
                                        ))
                                        : flatSchemaErrors.map((e: string, i: number) => (
                                            <Text key={i} fontSize="xs" mt={0.5}>• {e}</Text>
                                        ))
                                    }
                                    <Text fontSize="xs" color="red.600" mt={1} fontStyle="italic">
                                        Fix the compute expression or re-include the column upstream.
                                    </Text>
                                </Box>
                            </Alert>
                        )
                    })()}
                    <VStack align="stretch" spacing={2} mb={2}>
                        <HStack spacing={2}>
                            <FormControl size="sm">
                                <FormLabel fontSize="xs">Business Name</FormLabel>
                                <Input
                                    size="sm"
                                    value={businessName}
                                    onChange={(e) => setBusinessName(e.target.value)}
                                    placeholder="e.g., Calculate Totals"
                                />
                            </FormControl>
                            <FormControl size="sm">
                                <FormLabel fontSize="xs">Technical Name</FormLabel>
                                <Input
                                    size="sm"
                                    value={technicalName}
                                    onChange={(e) => setTechnicalName(e.target.value)}
                                    placeholder="Auto-generated"
                                    isReadOnly
                                    bg={useColorModeValue('gray.100', 'gray.700')}
                                />
                            </FormControl>
                        </HStack>
                    </VStack>
                    <Text fontSize="sm" color="gray.600">
                        Write executable code for DataFrame transformations. Input: _input_df (read-only), Output: _output_df (required)
                    </Text>
                </Box>

                <Box flex={1} overflowY="auto">
                    <Tabs
                        colorScheme="blue"
                        h="100%"
                        display="flex"
                        flexDirection="column"
                        index={tabIndex}
                        onChange={setTabIndex}
                    >
                        <TabList px={4} pt={2}>
                            <Tab>
                                <HStack spacing={2}>
                                    <Code size={14} />
                                    <Text>main.py</Text>
                                </HStack>
                            </Tab>
                            <Tab>
                                <HStack spacing={2}>
                                    <Code size={14} />
                                    <Text>requirements.txt</Text>
                                </HStack>
                            </Tab>
                            <Tab>
                                <HStack spacing={2}>
                                    <Eye size={14} />
                                    <Text>Output</Text>
                                </HStack>
                            </Tab>
                            <Tab>
                                <HStack spacing={2}>
                                    <AlertCircle size={14} />
                                    <Text>Errors</Text>
                                </HStack>
                            </Tab>
                        </TabList>

                        <TabPanels flex={1} overflowY="auto">
                            <TabPanel>
                                <VStack align="stretch" spacing={4}>
                                    <FormControl>
                                        <FormLabel fontSize="sm">Language</FormLabel>
                                        <Select
                                            size="sm"
                                            value={language}
                                            onChange={(e) => setLanguage(e.target.value)}
                                            isDisabled={true}
                                        >
                                            <option value="python">Python</option>
                                        </Select>
                                    </FormControl>

                                    <FormControl isRequired>
                                        <HStack justify="space-between" mb={2}>
                                            <FormLabel fontSize="sm" mb={0}>Code (main.py)</FormLabel>
                                            {/* Live updates: no per-node Save button */}
                                        </HStack>
                                        <Box
                                            borderWidth="1px"
                                            borderColor={borderColor}
                                            borderRadius="md"
                                            overflow="hidden"
                                            bg={codeBg}
                                            minH="400px"
                                        >
                                            <CodeMirror
                                                value={code}
                                                onChange={(value) => setCode(value)}
                                                height="400px"
                                                extensions={[python()]}
                                                theme={codeTheme}
                                                basicSetup={{
                                                    lineNumbers: true,
                                                    foldGutter: true,
                                                    dropCursor: false,
                                                    allowMultipleSelections: false,
                                                }}
                                                placeholder={`# Input DataFrame: _input_df (read-only, contains data from upstream node)
# Output DataFrame: _output_df (required, assign your result here)

_output_df = _input_df.copy()
# Add your transformations here`}
                                            />
                                        </Box>
                                    </FormControl>

                                    {availableColumns.length > 0 && (
                                        <Box>
                                            <Text fontSize="sm" fontWeight="semibold" mb={2}>
                                                Available Columns
                                            </Text>
                                            <Box maxH="200px" overflowY="auto" borderWidth="1px" borderColor={borderColor} borderRadius="md">
                                                <Table size="sm">
                                                    <Thead>
                                                        <Tr>
                                                            <Th>Column</Th>
                                                            <Th>Type</Th>
                                                        </Tr>
                                                    </Thead>
                                                    <Tbody>
                                                        {availableColumns.map((col) => (
                                                            <Tr key={col.name}>
                                                                <Td fontFamily="monospace" fontSize="xs">{col.name}</Td>
                                                                <Td fontSize="xs">{col.datatype}</Td>
                                                            </Tr>
                                                        ))}
                                                    </Tbody>
                                                </Table>
                                            </Box>
                                        </Box>
                                    )}
                                </VStack>
                            </TabPanel>

                            <TabPanel>
                                <VStack align="stretch" spacing={4}>
                                    <FormControl>
                                        <FormLabel fontSize="sm">Dependencies (requirements.txt)</FormLabel>
                                        <Text fontSize="xs" color="gray.600" mb={2}>
                                            Specify Python packages to install before running your code. One package per line.
                                        </Text>
                                        <Textarea
                                            value={requirements}
                                            onChange={(e) => setRequirements(e.target.value)}
                                            placeholder={`# Example dependencies:\nnumpy==1.24.0\nscikit-learn>=1.2.0\nrequests`}
                                            fontFamily="monospace"
                                            fontSize="sm"
                                            bg={codeBg}
                                            minH="300px"
                                            resize="vertical"
                                        />
                                    </FormControl>
                                    <Alert status="info" size="sm">
                                        <AlertIcon />
                                        <Text fontSize="xs">
                                            Dependencies will be installed in an isolated environment before code execution.
                                            Unsupported or malicious packages will cause execution to fail.
                                        </Text>
                                    </Alert>
                                </VStack>
                            </TabPanel>

                            <TabPanel>
                                {outputPreview ? (
                                    <Box>
                                        <Text fontSize="sm" color="gray.600" mb={2}>
                                            Preview: {outputPreview.rows?.length || 0} rows
                                        </Text>
                                        <Box overflow="auto" borderWidth="1px" borderColor={borderColor} borderRadius="md" maxH="400px">
                                            <Table size="sm" variant="simple">
                                                <Thead bg={useColorModeValue('gray.50', 'gray.700')}>
                                                    <Tr>
                                                        {(outputPreview.columns || []).map((col: any, idx: number) => (
                                                            <Th key={idx} textTransform="none" fontSize="xs" py={2}>
                                                                {col.name || col}
                                                            </Th>
                                                        ))}
                                                    </Tr>
                                                </Thead>
                                                <Tbody>
                                                    {(outputPreview.rows || []).slice(0, 50).map((row: any, rIdx: number) => (
                                                        <Tr key={rIdx}>
                                                            {(outputPreview.columns || []).map((col: any, cIdx: number) => {
                                                                const colName = col.name || col
                                                                const val = row[colName]
                                                                return (
                                                                    <Td key={cIdx} fontSize="xs" py={1} maxW="200px" isTruncated>
                                                                        {val === null ? <Text as="span" color="gray.400">null</Text> : String(val)}
                                                                    </Td>
                                                                )
                                                            })}
                                                        </Tr>
                                                    ))}
                                                </Tbody>
                                            </Table>
                                        </Box>
                                    </Box>
                                ) : (
                                    <Box textAlign="center" py={8}>
                                        <Text fontSize="sm" color="gray.500">
                                            No output yet. Execute the pipeline to see results.
                                        </Text>
                                    </Box>
                                )}
                            </TabPanel>

                            <TabPanel>
                                {errors.length > 0 ? (
                                    <VStack align="stretch" spacing={2}>
                                        {errors.map((error, idx) => (
                                            <Alert key={idx} status="error" size="sm">
                                                <AlertIcon />
                                                <Text fontSize="xs">{error}</Text>
                                            </Alert>
                                        ))}
                                    </VStack>
                                ) : (
                                    <Box textAlign="center" py={8}>
                                        <Text fontSize="sm" color="gray.500">
                                            No errors
                                        </Text>
                                    </Box>
                                )}
                            </TabPanel>
                        </TabPanels>
                    </Tabs>
                </Box>
            </Box>

            {/* Expanded Editor Modal */}
            <Modal isOpen={isExpanded} onClose={onCloseExpanded} size="full">
                <ModalOverlay />
                <ModalContent m={0} borderRadius={0}>
                    <ModalHeader borderBottomWidth="1px" borderColor={borderColor}>
                        <HStack justify="space-between">
                            <Text>Compute Node - Expanded Editor</Text>
                            <HStack>
                                <Button
                                    leftIcon={<FileCheck size={16} />}
                                    size="sm"
                                    colorScheme="purple"
                                    variant="outline"
                                    onClick={handleCompile}
                                    isLoading={compiling}
                                    loadingText="Compiling"
                                    isDisabled={loading || executing || !code.trim()}
                                    title="Validate code syntax and contracts without executing"
                                >
                                    Compile
                                </Button>
                                <Button
                                    leftIcon={<Play size={16} />}
                                    size="sm"
                                    colorScheme="blue"
                                    onClick={handleRun}
                                    isLoading={executing}
                                    loadingText="Running"
                                    isDisabled={loading || !code.trim()}
                                >
                                    Run
                                </Button>
                                {/* Live updates: no per-node Save button */}
                            </HStack>
                        </HStack>
                    </ModalHeader>
                    <ModalCloseButton />
                    <ModalBody p={0}>
                        <Tabs
                            colorScheme="blue"
                            h="100%"
                            display="flex"
                            flexDirection="column"
                            index={tabIndex}
                            onChange={setTabIndex}
                        >
                            <TabList px={4} pt={2}>
                                <Tab>
                                    <HStack spacing={2}>
                                        <Code size={14} />
                                        <Text>main.py</Text>
                                    </HStack>
                                </Tab>
                                <Tab>
                                    <HStack spacing={2}>
                                        <Code size={14} />
                                        <Text>requirements.txt</Text>
                                    </HStack>
                                </Tab>
                                <Tab>
                                    <HStack spacing={2}>
                                        <Eye size={14} />
                                        <Text>Output</Text>
                                    </HStack>
                                </Tab>
                                <Tab>
                                    <HStack spacing={2}>
                                        <AlertCircle size={14} />
                                        <Text>Errors</Text>
                                    </HStack>
                                </Tab>
                            </TabList>

                            <TabPanels flex={1} overflowY="auto">
                                <TabPanel h="100%">
                                    <VStack align="stretch" spacing={4} h="100%">
                                        <FormControl isRequired>
                                            <HStack justify="space-between" mb={2}>
                                                <FormLabel fontSize="sm" mb={0}>Code (main.py)</FormLabel>
                                                {/* Live updates: no per-node Save button */}
                                            </HStack>
                                            <Box
                                                borderWidth="1px"
                                                borderColor={borderColor}
                                                borderRadius="md"
                                                overflow="hidden"
                                                bg={codeBg}
                                                minH="calc(100vh - 250px)"
                                            >
                                                <CodeMirror
                                                    value={code}
                                                    onChange={(value) => setCode(value)}
                                                    height="calc(100vh - 250px)"
                                                    extensions={[python()]}
                                                    theme={codeTheme}
                                                    basicSetup={{
                                                        lineNumbers: true,
                                                        foldGutter: true,
                                                        dropCursor: false,
                                                        allowMultipleSelections: false,
                                                    }}
                                                    placeholder={`# Input DataFrame: _input_df (read-only, contains data from upstream node)
# Output DataFrame: _output_df (required, assign your result here)

_output_df = _input_df.copy()
# Add your transformations here`}
                                                />
                                            </Box>
                                        </FormControl>
                                    </VStack>
                                </TabPanel>

                                <TabPanel h="100%">
                                    <VStack align="stretch" spacing={4} h="100%">
                                        <FormControl>
                                            <FormLabel fontSize="sm">Dependencies (requirements.txt)</FormLabel>
                                            <Textarea
                                                value={requirements}
                                                onChange={(e) => setRequirements(e.target.value)}
                                                placeholder={`# Example dependencies:\nnumpy==1.24.0\nscikit-learn>=1.2.0\nrequests`}
                                                fontFamily="monospace"
                                                fontSize="sm"
                                                bg={codeBg}
                                                minH="calc(100vh - 250px)"
                                                resize="vertical"
                                            />
                                        </FormControl>
                                    </VStack>
                                </TabPanel>

                                <TabPanel>
                                    {outputPreview ? (
                                        <Box>
                                            <Text fontSize="sm" color="gray.600" mb={2}>
                                                Preview: {outputPreview.rows?.length || 0} rows
                                            </Text>
                                            <Box overflow="auto" borderWidth="1px" borderColor={borderColor} borderRadius="md" maxH="calc(100vh - 200px)">
                                                <Table size="sm" variant="simple">
                                                    <Thead bg={useColorModeValue('gray.50', 'gray.700')}>
                                                        <Tr>
                                                            {(outputPreview.columns || []).map((col: any, idx: number) => (
                                                                <Th key={idx}>{typeof col === 'string' ? col : col.name}</Th>
                                                            ))}
                                                        </Tr>
                                                    </Thead>
                                                    <Tbody>
                                                        {(outputPreview.rows || []).map((row: any, rowIdx: number) => (
                                                            <Tr key={rowIdx}>
                                                                {(outputPreview.columns || []).map((col: any, colIdx: number) => {
                                                                    const colName = typeof col === 'string' ? col : col.name
                                                                    return <Td key={colIdx}>{row[colName] !== null && row[colName] !== undefined ? String(row[colName]) : 'NULL'}</Td>
                                                                })}
                                                            </Tr>
                                                        ))}
                                                    </Tbody>
                                                </Table>
                                            </Box>
                                        </Box>
                                    ) : (
                                        <Box textAlign="center" py={8}>
                                            <Text fontSize="sm" color="gray.500">
                                                No output yet. Click Run to execute.
                                            </Text>
                                        </Box>
                                    )}
                                </TabPanel>

                                <TabPanel>
                                    {errors.length > 0 ? (
                                        <VStack align="stretch" spacing={2}>
                                            {errors.map((error, idx) => (
                                                <Alert key={idx} status="error" size="sm">
                                                    <AlertIcon />
                                                    <Text fontSize="xs">{error}</Text>
                                                </Alert>
                                            ))}
                                        </VStack>
                                    ) : (
                                        <Box textAlign="center" py={8}>
                                            <Text fontSize="sm" color="gray.500">
                                                No errors
                                            </Text>
                                        </Box>
                                    )}
                                </TabPanel>
                            </TabPanels>
                        </Tabs>
                    </ModalBody>
                </ModalContent>
            </Modal>
        </>
    )
}
