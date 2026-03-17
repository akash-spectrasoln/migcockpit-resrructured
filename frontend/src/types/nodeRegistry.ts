/**
 * Node Type Registry
 * Extensible system for registering and managing node types
 */

export type NodeCategory = 'source' | 'transform' | 'destination'

export interface NodeTypeDefinition {
  id: string
  category: NodeCategory
  label: string
  description: string
  icon: string // Icon name or component
  color: string // Tailwind color class
  defaultConfig: Record<string, any>
  configSchema: ConfigField[]
  validationRules?: ValidationRule[]
}

export interface ConfigField {
  name: string
  label: string
  type: 'text' | 'number' | 'select' | 'multiselect' | 'checkbox' | 'textarea' | 'json'
  required?: boolean
  placeholder?: string
  options?: { value: string; label: string }[]
  defaultValue?: any
  validation?: (value: any) => string | null // Returns error message or null
}

export interface ValidationRule {
  field: string
  rule: string
  message: string
  validator: (value: any, config: Record<string, any>) => boolean
}

/**
 * Node Type Registry
 * Add new node types here to make them available in the canvas
 */
export const NODE_TYPE_REGISTRY: Record<string, NodeTypeDefinition> = {
  // Source Nodes
  'source-mysql': {
    id: 'source-mysql',
    category: 'source',
    label: 'MySQL Source',
    description: 'Extract data from MySQL database',
    icon: 'Database',
    color: 'blue',
    defaultConfig: {
      sourceId: null,
      connectionType: 'mysql',
      tableName: '',
      schema: '',
      whereClause: '',
      limit: null,
    },
    configSchema: [
      {
        name: 'sourceId',
        label: 'Source Connection',
        type: 'select',
        required: true,
        options: [], // Populated from API
      },
      {
        name: 'tableName',
        label: 'Table Name',
        type: 'text',
        required: true,
        placeholder: 'Enter table name',
      },
      {
        name: 'schema',
        label: 'Schema/Database',
        type: 'text',
        placeholder: 'Optional schema name',
      },
      {
        name: 'whereClause',
        label: 'WHERE Clause',
        type: 'textarea',
        placeholder: 'e.g., status = "active"',
      },
      {
        name: 'limit',
        label: 'Row Limit',
        type: 'number',
        placeholder: 'Leave empty for all rows',
      },
    ],
  },
  'source-oracle': {
    id: 'source-oracle',
    category: 'source',
    label: 'Oracle Source',
    description: 'Extract data from Oracle database',
    icon: 'Database',
    color: 'blue',
    defaultConfig: {
      sourceId: null,
      connectionType: 'oracle',
      tableName: '',
      schema: '',
      whereClause: '',
    },
    configSchema: [
      {
        name: 'sourceId',
        label: 'Source Connection',
        type: 'select',
        required: true,
        options: [],
      },
      {
        name: 'tableName',
        label: 'Table Name',
        type: 'text',
        required: true,
      },
      {
        name: 'schema',
        label: 'Schema',
        type: 'text',
      },
    ],
  },
  'source-sqlserver': {
    id: 'source-sqlserver',
    category: 'source',
    label: 'SQL Server Source',
    description: 'Extract data from SQL Server database',
    icon: 'Database',
    color: 'blue',
    defaultConfig: {
      sourceId: null,
      connectionType: 'sqlserver',
      tableName: '',
      schema: '',
      whereClause: '',
    },
    configSchema: [
      {
        name: 'sourceId',
        label: 'Source Connection',
        type: 'select',
        required: true,
        options: [],
      },
      {
        name: 'tableName',
        label: 'Table Name',
        type: 'text',
        required: true,
      },
    ],
  },

  // Transform Nodes
  'transform-map': {
    id: 'transform-map',
    category: 'transform',
    label: 'Data Mapping',
    description: 'Map source columns to destination columns',
    icon: 'Settings',
    color: 'purple',
    defaultConfig: {
      transformType: 'map',
      mappings: [],
    },
    configSchema: [
      {
        name: 'mappings',
        label: 'Column Mappings',
        type: 'json',
        required: true,
        placeholder: 'Array of {source: "col1", target: "col2"}',
      },
    ],
  },
  'transform-filter': {
    id: 'transform-filter',
    category: 'transform',
    label: 'Filter',
    description: 'Filter rows based on conditions',
    icon: 'Settings',
    color: 'purple',
    defaultConfig: {
      transformType: 'filter',
      conditions: [],
      operator: 'AND', // AND or OR
    },
    configSchema: [
      {
        name: 'operator',
        label: 'Condition Operator',
        type: 'select',
        required: true,
        options: [
          { value: 'AND', label: 'AND (All conditions)' },
          { value: 'OR', label: 'OR (Any condition)' },
        ],
        defaultValue: 'AND',
      },
      {
        name: 'conditions',
        label: 'Filter Conditions',
        type: 'json',
        required: true,
      },
    ],
  },
  'filter': {
    id: 'filter',
    category: 'transform',
    label: 'Filter',
    description: 'Filter data rows using multiple conditions',
    icon: 'Filter',
    color: 'purple',
    defaultConfig: {
      conditions: [],
    },
    configSchema: [
      {
        name: 'conditions',
        label: 'Filter Conditions',
        type: 'json',
        required: true,
        placeholder: 'Array of filter conditions',
      },
    ],
  },
  'join': {
    id: 'join',
    category: 'transform',
    label: 'Join',
    description: 'Join two tables based on conditions',
    icon: 'GitMerge',
    color: 'purple',
    defaultConfig: {
      joinType: 'INNER',
      conditions: [],
    },
    configSchema: [
      {
        name: 'joinType',
        label: 'Join Type',
        type: 'select',
        required: true,
        options: [
          { value: 'INNER', label: 'INNER JOIN' },
          { value: 'LEFT', label: 'LEFT JOIN' },
          { value: 'RIGHT', label: 'RIGHT JOIN' },
          { value: 'FULL OUTER', label: 'FULL OUTER JOIN' },
          { value: 'CROSS', label: 'CROSS JOIN' },
        ],
        defaultValue: 'INNER',
      },
      {
        name: 'conditions',
        label: 'Join Conditions',
        type: 'json',
        required: true,
        placeholder: 'Array of join conditions',
      },
    ],
  },
  'projection': {
    id: 'projection',
    category: 'transform',
    label: 'Projection',
    description: 'Select and rename output columns',
    icon: 'Columns',
    color: 'purple',
    defaultConfig: {
      selectedColumns: [],
      columnMappings: [],
    },
    configSchema: [
      {
        name: 'selectedColumns',
        label: 'Selected Columns',
        type: 'json',
        required: true,
        placeholder: 'Array of column names',
      },
    ],
  },
  'calculated': {
    id: 'calculated',
    category: 'transform',
    label: 'Calculated Columns',
    description: 'Create new columns with expressions',
    icon: 'Calculator',
    color: 'purple',
    defaultConfig: {
      calculatedColumns: [],
    },
    configSchema: [
      {
        name: 'calculatedColumns',
        label: 'Calculated Columns',
        type: 'json',
        required: true,
        placeholder: 'Array of calculated column definitions',
      },
    ],
  },
  'transform-clean': {
    id: 'transform-clean',
    category: 'transform',
    label: 'Data Cleaning',
    description: 'Clean and normalize data',
    icon: 'Settings',
    color: 'purple',
    defaultConfig: {
      transformType: 'clean',
      rules: [],
    },
    configSchema: [
      {
        name: 'rules',
        label: 'Cleaning Rules',
        type: 'json',
        required: true,
      },
    ],
  },
  'transform-validate': {
    id: 'transform-validate',
    category: 'transform',
    label: 'Validation',
    description: 'Validate data against business rules',
    icon: 'Settings',
    color: 'purple',
    defaultConfig: {
      transformType: 'validate',
      rules: [],
    },
    configSchema: [
      {
        name: 'rules',
        label: 'Validation Rules',
        type: 'json',
        required: true,
      },
    ],
  },

  // Destination Nodes
  'destination-hana': {
    id: 'destination-hana',
    category: 'destination',
    label: 'SAP HANA',
    description: 'Load data to SAP HANA database',
    icon: 'ArrowRight',
    color: 'green',
    defaultConfig: {
      destinationId: null,
      connectionType: 'hana',
      tableName: '',
      schema: '',
      loadMode: 'insert', // insert, upsert, replace, drop_and_reload
    },
    configSchema: [
      {
        name: 'destinationId',
        label: 'Destination Connection',
        type: 'select',
        required: true,
        options: [],
      },
      {
        name: 'tableName',
        label: 'Target Table',
        type: 'text',
        required: true,
        placeholder: 'New table name (will be created) or existing table name',
      },
      {
        name: 'schema',
        label: 'Schema',
        type: 'text',
      },
      {
        name: 'loadMode',
        label: 'Load Mode',
        type: 'select',
        required: true,
        options: [
          { value: 'insert', label: 'Insert' },
          { value: 'upsert', label: 'Upsert' },
          { value: 'replace', label: 'Replace' },
          { value: 'drop_and_reload', label: 'Drop and reload' },
        ],
        defaultValue: 'insert',
      },
    ],
  },

  'destination-postgresql': {
    id: 'destination-postgresql',
    category: 'destination',
    label: 'PostgreSQL',
    description: 'Load data to PostgreSQL database',
    icon: 'ArrowRight',
    color: 'blue',
    defaultConfig: {
      destinationId: null,
      connectionType: 'postgresql',
      tableName: '',
      schema: 'public',
      loadMode: 'insert', // insert, upsert, replace, drop_and_reload
    },
    configSchema: [
      {
        name: 'destinationId',
        label: 'Destination Connection',
        type: 'select',
        required: true,
        options: [],
      },
      {
        name: 'tableName',
        label: 'Target Table',
        type: 'text',
        required: true,
        placeholder: 'New table name (will be created) or existing table name',
      },
      {
        name: 'schema',
        label: 'Schema',
        type: 'text',
        defaultValue: 'public',
      },
      {
        name: 'loadMode',
        label: 'Load Mode',
        type: 'select',
        required: true,
        options: [
          { value: 'insert', label: 'Insert' },
          { value: 'upsert', label: 'Upsert' },
          { value: 'replace', label: 'Replace' },
          { value: 'drop_and_reload', label: 'Drop and reload' },
        ],
        defaultValue: 'insert',
      },
    ],
  },
  'aggregate': {
    id: 'aggregate',
    category: 'transform',
    label: 'Aggregate',
    description: 'Group data and perform aggregate functions',
    icon: 'BarChart3',
    color: 'purple',
    defaultConfig: {
      groupBy: [],
      aggregateColumns: [],
    },
    configSchema: [
      {
        name: 'groupBy',
        label: 'Group By Columns',
        type: 'multiselect',
        required: true,
      },
      {
        name: 'aggregateColumns',
        label: 'Aggregations',
        type: 'json',
        required: true,
        placeholder: 'Array of {column: "col1", function: "SUM", alias: "total_col1"}',
      },
    ],
    validationRules: [
      {
        field: 'groupBy',
        rule: 'required',
        message: 'At least one group by column is required',
        validator: (value: any) => Array.isArray(value) && value.length > 0,
      },
    ],
  },
  'compute': {
    id: 'compute',
    category: 'transform',
    label: 'Compute',
    description: 'Execute custom code for DataFrame transformations',
    icon: 'Code2',
    color: 'purple',
    defaultConfig: {
      code: '',
      language: 'python',
    },
    configSchema: [
      {
        name: 'language',
        label: 'Language',
        type: 'select',
        required: true,
        options: [
          { value: 'python', label: 'Python' },
        ],
        defaultValue: 'python',
      },
      {
        name: 'code',
        label: 'Code',
        type: 'textarea',
        required: true,
        placeholder: '# Input: input_df\n# Output: output_df\n\noutput_df = input_df.copy()',
      },
    ],
    validationRules: [
      {
        field: 'code',
        rule: 'required',
        message: 'Code cannot be empty',
        validator: (value: any) => typeof value === 'string' && value.trim().length > 0,
      },
    ],
  },
}

/**
 * Get node type definition by ID
 */
export function getNodeTypeDefinition(nodeTypeId: string): NodeTypeDefinition | undefined {
  return NODE_TYPE_REGISTRY[nodeTypeId]
}

/**
 * Get all node types by category
 */
export function getNodeTypesByCategory(category: NodeCategory): NodeTypeDefinition[] {
  return Object.values(NODE_TYPE_REGISTRY).filter((def) => def.category === category)
}

/**
 * Register a new node type (for extensibility)
 */
export function registerNodeType(definition: NodeTypeDefinition): void {
  NODE_TYPE_REGISTRY[definition.id] = definition
}

