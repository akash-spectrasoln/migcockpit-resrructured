/**
 * Type-safe filter system with proper data type handling
 * Prevents implicit string coercion in database queries
 */

export type ColumnDataType = 
  | 'INTEGER' | 'BIGINT' | 'SMALLINT' | 'INT' | 'INT2' | 'INT4' | 'INT8'
  | 'NUMERIC' | 'DECIMAL' | 'FLOAT' | 'DOUBLE' | 'REAL' | 'FLOAT4' | 'FLOAT8' | 'DOUBLE PRECISION'
  | 'BOOLEAN' | 'BOOL'
  | 'DATE' | 'TIMESTAMP' | 'TIMESTAMPTZ' | 'TIMESTAMP WITH TIME ZONE' | 'TIMESTAMP WITHOUT TIME ZONE'
  | 'TIME' | 'TIMETZ' | 'TIME WITH TIME ZONE' | 'TIME WITHOUT TIME ZONE'
  | 'TEXT' | 'VARCHAR' | 'CHAR' | 'CHARACTER' | 'CHARACTER VARYING'
  | 'JSON' | 'JSONB' | 'UUID' | 'BYTEA' | 'ARRAY';

export interface ColumnMetadata {
  /** Human-friendly business name used for display. */
  name: string;
  /** Explicit business name; falls back to `name` when not provided. */
  business_name?: string;
  /** Stable identifier for lineage and config; use when persisting. Display uses `business_name`/`name`. */
  technical_name?: string;
  /** Actual DB column name; used for fetch. Optional in UI; backend sets at source. */
  db_name?: string;
  datatype: ColumnDataType;
  nullable: boolean;
}

export interface TypedFilterCondition {
  id: string;
  column: string;
  operator: string;
  value: any;  // Type-safe value
  logicalOperator?: 'AND' | 'OR';
  _columnType?: ColumnDataType;  // Internal: track column type for validation
}

/**
 * Parse a string value to its proper type based on column data type
 * This prevents implicit string coercion in SQL queries
 */
export function parseValueByType(value: string | any, dataType: ColumnDataType | undefined): any {
  // If already not a string, return as-is (handles programmatic values)
  if (typeof value !== 'string') {
    return value;
  }

  // Handle empty/null values
  if (!value || value.trim() === '') {
    return null;
  }

  const trimmedValue = value.trim();
  
  // ✅ DEFENSIVE: Handle missing/undefined datatype - default to TEXT
  if (!dataType) {
    console.warn('parseValueByType: dataType is undefined, defaulting to TEXT');
    return trimmedValue;
  }
  
  const upperType = dataType.toUpperCase();

  // ===== NUMERIC TYPES =====
  if (
    upperType.includes('INT') || 
    upperType === 'SMALLINT' || 
    upperType === 'BIGINT'
  ) {
    const parsed = parseInt(trimmedValue, 10);
    if (isNaN(parsed)) {
      throw new Error(`Invalid integer value "${value}" for type ${dataType}`);
    }
    return parsed;
  }

  if (
    upperType.includes('NUMERIC') || 
    upperType.includes('DECIMAL') || 
    upperType.includes('FLOAT') || 
    upperType.includes('DOUBLE') || 
    upperType === 'REAL'
  ) {
    const parsed = parseFloat(trimmedValue);
    if (isNaN(parsed)) {
      throw new Error(`Invalid numeric value "${value}" for type ${dataType}`);
    }
    return parsed;
  }

  // ===== BOOLEAN TYPES =====
  if (upperType === 'BOOLEAN' || upperType === 'BOOL') {
    const lower = trimmedValue.toLowerCase();
    if (lower === 'true' || lower === '1' || lower === 'yes' || lower === 't') {
      return true;
    }
    if (lower === 'false' || lower === '0' || lower === 'no' || lower === 'f') {
      return false;
    }
    throw new Error(`Invalid boolean value "${value}". Use true/false, 1/0, or yes/no`);
  }

  // ===== DATE/TIME TYPES =====
  // Keep as string - database will parse and validate
  // But perform basic validation
  if (
    upperType.includes('DATE') || 
    upperType.includes('TIMESTAMP') || 
    upperType.includes('TIME')
  ) {
    // Basic ISO date validation
    if (upperType.includes('DATE') || upperType.includes('TIMESTAMP')) {
      // Allow formats: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS
      const dateRegex = /^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?/;
      if (!dateRegex.test(trimmedValue)) {
        throw new Error(`Invalid date format "${value}". Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS`);
      }
    }
    return trimmedValue;
  }

  // ===== TEXT TYPES =====
  // Return as-is (already a string)
  return trimmedValue;
}

/**
 * Parse array of values (for IN, NOT IN, BETWEEN operators)
 */
export function parseArrayValuesByType(values: any[], dataType: ColumnDataType): any[] {
  if (!Array.isArray(values)) {
    return [];
  }

  const results: any[] = [];
  for (const value of values) {
    try {
      const parsed = parseValueByType(value, dataType);
      if (parsed !== null) {
        results.push(parsed);
      }
    } catch (error) {
      console.warn(`Skipping invalid value "${value}": ${error}`);
      // Continue parsing other values
    }
  }
  return results;
}

/**
 * Validate and parse filter condition value based on column type
 * Handles special operators (IN, BETWEEN, etc.)
 */
export function parseFilterValue(
  value: any, 
  operator: string, 
  dataType: ColumnDataType | undefined
): any {
  // NULL operators don't need values
  if (operator === 'IS NULL' || operator === 'IS NOT NULL') {
    return null;
  }

  // BETWEEN operator expects array [min, max]
  if (operator === 'BETWEEN') {
    let arrayValue: any[];
    
    if (typeof value === 'string') {
      // Try JSON parse first
      try {
        arrayValue = JSON.parse(value);
      } catch {
        // Fallback to comma-separated
        arrayValue = value.split(',').map(v => v.trim());
      }
    } else if (Array.isArray(value)) {
      arrayValue = value;
    } else {
      throw new Error(`BETWEEN operator requires array [min, max], got: ${typeof value}`);
    }

    if (arrayValue.length !== 2) {
      throw new Error(`BETWEEN operator requires exactly 2 values, got: ${arrayValue.length}`);
    }

    return parseArrayValuesByType(arrayValue, dataType);
  }

  // IN / NOT IN operators expect array
  if (operator === 'IN' || operator === 'NOT IN') {
    let arrayValue: any[];

    if (typeof value === 'string') {
      // Comma-separated values
      arrayValue = value.split(',').map(v => v.trim()).filter(v => v);
    } else if (Array.isArray(value)) {
      arrayValue = value;
    } else {
      arrayValue = [value];
    }

    return parseArrayValuesByType(arrayValue, dataType);
  }

  // Single value operators
  return parseValueByType(value, dataType);
}

/**
 * Get input type for HTML input based on column data type
 */
export function getInputTypeForColumn(dataType: ColumnDataType | undefined): string {
  // ✅ DEFENSIVE: Handle missing datatype
  if (!dataType) {
    return 'text';
  }
  
  const upperType = dataType.toUpperCase();

  if (
    upperType.includes('INT') || 
    upperType.includes('NUMERIC') || 
    upperType.includes('DECIMAL') || 
    upperType.includes('FLOAT') || 
    upperType.includes('DOUBLE') || 
    upperType === 'REAL'
  ) {
    return 'number';
  }

  if (upperType === 'BOOLEAN' || upperType === 'BOOL') {
    return 'checkbox';  // Or use select
  }

  if (upperType.includes('DATE')) {
    return 'date';
  }

  if (upperType.includes('TIME')) {
    return 'time';
  }

  return 'text';
}

/**
 * Validate filter condition before sending to backend
 */
export function validateFilterCondition(
  condition: TypedFilterCondition,
  availableColumns: ColumnMetadata[]
): { valid: boolean; error?: string } {
  if (!condition.column) {
    return { valid: false, error: 'Column is required' };
  }

  if (!condition.operator) {
    return { valid: false, error: 'Operator is required' };
  }

  // Find column metadata
  const columnMeta = availableColumns.find(c => c.name === condition.column);
  if (!columnMeta) {
    return { valid: false, error: `Column "${condition.column}" not found` };
  }

  // Check if operator requires a value
  const noValueOps = ['IS NULL', 'IS NOT NULL'];
  if (!noValueOps.includes(condition.operator)) {
    if (condition.value === null || condition.value === undefined || condition.value === '') {
      return { valid: false, error: `Value is required for operator "${condition.operator}"` };
    }
  }

  // Validate value type
  try {
    parseFilterValue(condition.value, condition.operator, columnMeta.datatype);
  } catch (error: any) {
    return { valid: false, error: error.message };
  }

  return { valid: true };
}
