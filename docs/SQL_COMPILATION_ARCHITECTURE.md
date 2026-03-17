# SQL Compilation Architecture: Multiple Nodes Explained

## Overview

The SQL compilation system compiles an entire pipeline DAG (Directed Acyclic Graph) into a **single SQL query** using Common Table Expressions (CTEs). This document explains how multiple nodes are processed and combined.

## Core Principle

**Each node preview executes exactly ONE SQL query** that represents the entire pipeline from sources up to the selected node.

## Example Pipeline

Let's trace through a complex pipeline with multiple nodes:

```
┌─────────┐
│ Source1 │ (users table)
└────┬────┘
     │
     │
┌────▼────┐
│ Filter1 │ (age > 18)
└────┬────┘
     │
     │
┌────▼────┐      ┌─────────┐
│  Join1  │◄─────│ Source2 │ (orders table)
└────┬────┘      └─────────┘
     │
     │
┌────▼────┐
│Filter2  │ (order_total > 100)
└────┬────┘
     │
     │
┌────▼────┐
│Projection│ (select: user_id, order_id, total)
└─────────┘
```

## Step-by-Step Compilation Process

### Step 1: DAG Validation and Upstream Discovery

When previewing the **Projection** node:

```python
target_node_id = "projection1"
upstream_nodes = find_upstream_nodes(nodes, edges, target_node_id)
# Result: ["source1", "filter1", "source2", "join1", "filter2", "projection1"]
```

**Key Points:**
- All nodes required to compute the target are identified
- Nodes are returned in **topological order** (dependencies first)
- Source nodes come before nodes that depend on them

### Step 2: CTE Generation (Bottom-Up)

The compiler builds CTEs for each upstream node in order:

#### CTE 1: Source1 (users table)

```sql
node_source1 AS (
    SELECT * FROM "public"."users"
)
```

**Metadata:**
```python
{
    'columns': [
        {'name': 'id', 'datatype': 'INTEGER'},
        {'name': 'name', 'datatype': 'TEXT'},
        {'name': 'age', 'datatype': 'INTEGER'}
    ]
}
```

#### CTE 2: Filter1 (age > 18)

```sql
node_filter1 AS (
    SELECT * 
    FROM node_source1 
    WHERE "age" > %s
)
```

**Parameters:** `[18]`

**Metadata:** (Same as source1 - filter doesn't change schema)

#### CTE 3: Source2 (orders table)

```sql
node_source2 AS (
    SELECT * FROM "public"."orders"
)
```

**Metadata:**
```python
{
    'columns': [
        {'name': 'order_id', 'datatype': 'INTEGER'},
        {'name': 'user_id', 'datatype': 'INTEGER'},
        {'name': 'total', 'datatype': 'DECIMAL'}
    ]
}
```

#### CTE 4: Join1 (INNER JOIN)

```sql
node_join1 AS (
    SELECT 
        __L__."id" AS "user_id",
        __R__."order_id" AS "order_id",
        __R__."total" AS "total"
    FROM node_filter1 AS __L__
    INNER JOIN node_source2 AS __R__
    ON __L__."id" = __R__."user_id"
)
```

**Key Points:**
- Left side (`__L__`) references `node_filter1` (already filtered users)
- Right side (`__R__`) references `node_source2` (orders)
- Output columns are explicitly selected
- Join conditions reference columns from both CTEs

**Metadata:**
```python
{
    'columns': [
        {'name': 'user_id', 'datatype': 'INTEGER'},
        {'name': 'order_id', 'datatype': 'INTEGER'},
        {'name': 'total', 'datatype': 'DECIMAL'}
    ]
}
```

#### CTE 5: Filter2 (order_total > 100)

```sql
node_filter2 AS (
    SELECT * 
    FROM node_join1 
    WHERE "total" > %s
)
```

**Parameters:** `[18, 100]` (accumulated from previous filters)

**Metadata:** (Same as join1)

#### CTE 6: Projection (select specific columns)

```sql
node_projection1 AS (
    SELECT 
        "user_id",
        "order_id",
        "total"
    FROM node_filter2
)
```

**Metadata:**
```python
{
    'columns': [
        {'name': 'user_id', 'datatype': 'INTEGER'},
        {'name': 'order_id', 'datatype': 'INTEGER'},
        {'name': 'total', 'datatype': 'DECIMAL'}
    ]
}
```

### Step 3: Final Query Assembly

All CTEs are combined with the final SELECT:

```sql
WITH 
    node_source1 AS (
        SELECT * FROM "public"."users"
    ),
    node_filter1 AS (
        SELECT * 
        FROM node_source1 
        WHERE "age" > %s
    ),
    node_source2 AS (
        SELECT * FROM "public"."orders"
    ),
    node_join1 AS (
        SELECT 
            __L__."id" AS "user_id",
            __R__."order_id" AS "order_id",
            __R__."total" AS "total"
        FROM node_filter1 AS __L__
        INNER JOIN node_source2 AS __R__
        ON __L__."id" = __R__."user_id"
    ),
    node_filter2 AS (
        SELECT * 
        FROM node_join1 
        WHERE "total" > %s
    ),
    node_projection1 AS (
        SELECT 
            "user_id",
            "order_id",
            "total"
        FROM node_filter2
    )
SELECT 
    "user_id",
    "order_id",
    "total"
FROM node_projection1
LIMIT %s
```

**Final Parameters:** `[18, 100, 50]` (page_size = 50)

## Visual Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Preview Request                              │
│  Target Node: projection1                                       │
│  Preview Mode: true                                             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 1: DAG Validation                             │
│  • Validate no cycles                                            │
│  • Find upstream nodes: [source1, filter1, source2,           │
│                          join1, filter2, projection1]          │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│         Step 2: Build CTEs (Topological Order)                   │
│                                                                  │
│  ┌──────────────┐                                              │
│  │ CTE: source1  │──┐                                            │
│  └──────────────┘  │                                            │
│                    │                                            │
│  ┌──────────────┐  │                                            │
│  │ CTE: filter1 │──┼──┐                                         │
│  └──────────────┘  │  │                                         │
│                    │  │                                         │
│  ┌──────────────┐  │  │                                         │
│  │ CTE: source2  │──┼──┼──┐                                     │
│  └──────────────┘  │  │  │                                     │
│                    │  │  │                                     │
│  ┌──────────────┐  │  │  │                                     │
│  │ CTE: join1   │──┼──┼──┘                                     │
│  └──────────────┘  │  │                                         │
│                    │  │                                         │
│  ┌──────────────┐  │  │                                         │
│  │ CTE: filter2 │──┼──┘                                         │
│  └──────────────┘  │                                            │
│                    │                                            │
│  ┌──────────────┐  │                                            │
│  │ CTE: proj1   │──┘                                            │
│  └──────────────┘                                              │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 3: Final SELECT                               │
│  SELECT columns FROM node_projection1 LIMIT %s                 │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 4: Execute Query                             │
│  • Connect to source database                                   │
│  • Execute compiled SQL with parameters                         │
│  • Return rows, columns, metadata                              │
└─────────────────────────────────────────────────────────────────┘
```

## Key Architectural Rules

### 1. Single Query Execution

✅ **Correct:** One SQL query with CTEs  
❌ **Wrong:** Multiple queries executed sequentially

### 2. LIMIT Placement

✅ **Correct:** LIMIT only in final SELECT  
❌ **Wrong:** LIMIT in CTEs (would break joins/aggregates)

```sql
-- ✅ CORRECT
WITH cte1 AS (SELECT * FROM table1),
     cte2 AS (SELECT * FROM cte1)
SELECT * FROM cte2 LIMIT 50

-- ❌ WRONG
WITH cte1 AS (SELECT * FROM table1 LIMIT 50),  -- BAD!
     cte2 AS (SELECT * FROM cte1)
SELECT * FROM cte2
```

### 3. Metadata Propagation

Metadata flows through the pipeline:

```
Source Metadata
    │
    ├─► Filter Metadata (unchanged)
    │       │
    │       └─► Join Metadata (combines left + right)
    │               │
    │               ├─► Filter Metadata (unchanged)
    │               │       │
    │               │       └─► Projection Metadata (selected columns)
```

### 4. Parameter Accumulation

Parameters accumulate as filters are added:

- Filter1: `[18]`
- Filter2: `[18, 100]`
- Final: `[18, 100, 50]` (page_size)

## Complex Scenarios

### Scenario 1: Multiple Joins

```
Source1 ──┐
          ├──► Join1 ──┐
Source2 ──┘             ├──► Join2 ──► Projection
                        │
Source3 ────────────────┘
```

**CTE Order:**
1. `node_source1`
2. `node_source2`
3. `node_join1` (source1 + source2)
4. `node_source3`
5. `node_join2` (join1 + source3)
6. `node_projection`

### Scenario 2: Calculated Columns

```
Source ──► Projection (with calculated columns)
```

**Projection CTE:**
```sql
node_projection1 AS (
    SELECT 
        "id",
        "name",
        UPPER("name") AS "upper_name",
        LENGTH("name") AS "name_length"
    FROM node_source1
)
```

**Key Points:**
- Base columns selected directly
- Calculated columns use SQL expressions
- Expression translator converts Python-style to SQL

### Scenario 3: Aggregates

```
Source ──► Filter ──► Aggregate ──► Projection
```

**Aggregate CTE:**
```sql
node_aggregate1 AS (
    SELECT 
        "category",
        COUNT(*) AS "count",
        SUM("amount") AS "total"
    FROM node_filter1
    GROUP BY "category"
)
```

**Key Points:**
- GROUP BY columns included in SELECT
- Aggregate functions applied
- Metadata reflects aggregate columns

## Execution Flow in Code

```python
# 1. Validate DAG
is_valid, error = validate_dag(nodes, edges)

# 2. Find upstream nodes
upstream_nodes = find_upstream_nodes(nodes, edges, target_node_id)
# Result: ["source1", "filter1", "source2", "join1", "filter2", "projection1"]

# 3. Initialize compiler
compiler = SQLCompiler(nodes, edges, target_node_id, customer, db_type)

# 4. Build CTEs for each upstream node
for node_id in upstream_nodes:
    node = node_map[node_id]
    node_type = node['data']['type']
    
    if node_type == 'source':
        cte_sql, metadata = compiler._build_source_cte(node)
    elif node_type == 'filter':
        cte_sql, metadata = compiler._build_filter_cte(node)
    elif node_type == 'join':
        cte_sql, metadata = compiler._build_join_cte(node)
    # ... etc
    
    # Store CTE name and metadata
    cte_map[node_id] = cte_name
    metadata_map[node_id] = metadata

# 5. Build final SELECT
final_cte = cte_map[target_node_id]
final_metadata = metadata_map[target_node_id]
select_clause = build_select_from_metadata(final_metadata)

# 6. Combine into single query
query = f"WITH {', '.join(ctes)}\nSELECT {select_clause}\nFROM {final_cte}\nLIMIT %s"

# 7. Execute
results = execute_preview_query(query, params, source_config, page, page_size)
```

## Benefits of This Architecture

1. **Performance:** Single query is faster than multiple round-trips
2. **Accuracy:** Results match production execution exactly
3. **Consistency:** No intermediate caching or state management
4. **Simplicity:** One query, one execution, one result set
5. **Database Optimization:** Database can optimize entire pipeline

## Comparison: Old vs New

### Old Architecture (Sequential Execution)

```
Preview Projection:
  1. Execute Source1 → Cache results
  2. Execute Filter1 on cached Source1 → Cache results
  3. Execute Source2 → Cache results
  4. Execute Join1 on cached Filter1 + Source2 → Cache results
  5. Execute Filter2 on cached Join1 → Cache results
  6. Execute Projection on cached Filter2 → Return results
```

**Problems:**
- Multiple database queries
- Intermediate caching required
- Potential inconsistency
- Slower performance

### New Architecture (Single Query)

```
Preview Projection:
  1. Compile entire pipeline to single SQL query
  2. Execute one query → Return results
```

**Benefits:**
- Single database query
- No caching needed
- Guaranteed consistency
- Better performance

## Summary

The SQL compilation system:

1. **Validates** the pipeline DAG
2. **Discovers** all upstream nodes in topological order
3. **Builds** CTEs for each node (bottom-up)
4. **Combines** CTEs into single SQL query
5. **Executes** query and returns results

This ensures that previewing any node shows the exact result of executing the entire pipeline up to that node, using a single, optimized SQL query.
