import { describe, expect, it } from 'vitest'

import { compilePipeline } from '../compiler'
import type { RawEdge, RawNode } from '../types'

const pos = { x: 0, y: 0 }

function node(id: string, type: string, config: Record<string, unknown> = {}, extra: Record<string, unknown> = {}): RawNode {
  return {
    id,
    type,
    position: pos,
    data: {
      type,
      config,
      ...extra,
    },
  }
}

describe('pipeline-engine integration', () => {
  it('keeps filter output schema aligned with upstream projection columns', () => {
    const nodes: RawNode[] = [
      node('src', 'source', {}, {
        output_metadata: {
          columns: [
            { name: 'id', datatype: 'INTEGER' },
            { name: 'start_time', datatype: 'TIMESTAMP' },
            { name: 'del_rec', datatype: 'INTEGER' },
          ],
        },
      }),
      node('proj', 'projection', {
        selectedColumns: ['id', 'start_time'],
        calculatedColumns: [{ name: 'time', expression: 'CAST(start_time AS VARCHAR)' }],
      }),
      node('flt', 'filter', {
        mode: 'expression',
        expression: 'time IS NOT NULL',
      }),
    ]
    const edges: RawEdge[] = [
      { id: 'e1', source: 'src', target: 'proj' },
      { id: 'e2', source: 'proj', target: 'flt' },
    ]

    const compiled = compilePipeline(nodes, edges)
    const projectionOutput = compiled.nodes.proj.outputSchema.map((c) => c.name)
    const filterOutput = compiled.nodes.flt.outputSchema.map((c) => c.name)

    expect(projectionOutput).toEqual(['id', 'start_time', 'time'])
    expect(filterOutput).toEqual(projectionOutput)
  })

  it('uses in-memory join outputColumns (unsaved edit style) immediately in compiled output', () => {
    const left = node('left', 'source', {}, {
      output_metadata: { columns: [{ name: 'connection_id' }, { name: 'status' }] },
    })
    const right = node('right', 'source', {}, {
      output_metadata: { columns: [{ name: 'connection_id' }, { name: 'table_name' }] },
    })
    const join = node('join', 'join', {
      outputColumns: [
        { source: 'left', column: 'connection_id', outputName: '_L_connection_id', included: true },
        { source: 'right', column: 'connection_id', outputName: '_R_connection_id', included: true },
        { source: 'left', column: 'status', outputName: 'status', included: false },
        { source: 'right', column: 'table_name', outputName: 'table_name', included: true },
      ],
    })

    const nodes: RawNode[] = [left, right, join]
    const edges: RawEdge[] = [
      { id: 'jl', source: 'left', target: 'join', targetHandle: 'left' },
      { id: 'jr', source: 'right', target: 'join', targetHandle: 'right' },
    ]

    const compiled = compilePipeline(nodes, edges)
    const names = compiled.nodes.join.outputSchema.map((c) => c.name)
    expect(names).toEqual(['_L_connection_id', '_R_connection_id', 'table_name'])
    expect(names).not.toContain('status')
  })

  it('propagates compute output metadata to downstream nodes', () => {
    const nodes: RawNode[] = [
      node('src', 'source', {}, {
        output_metadata: {
          columns: [{ name: 'id' }, { name: 'del_rec' }],
        },
      }),
      node('cmp', 'compute', { code: '_output_df = _input_df.copy()' }, {
        output_metadata: {
          columns: [{ name: 'id' }, { name: 'del_rec' }, { name: 'risk_flag' }],
        },
      }),
      node('proj', 'projection', { selectedColumns: ['id', 'risk_flag'] }),
    ]
    const edges: RawEdge[] = [
      { id: 'e1', source: 'src', target: 'cmp' },
      { id: 'e2', source: 'cmp', target: 'proj' },
    ]

    const compiled = compilePipeline(nodes, edges)
    expect(compiled.nodes.cmp.outputSchema.map((c) => c.name)).toEqual(['id', 'del_rec', 'risk_flag'])
    expect(compiled.nodes.proj.outputSchema.map((c) => c.name)).toEqual(['id', 'risk_flag'])
  })
})
