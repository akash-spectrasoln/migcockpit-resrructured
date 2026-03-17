import React, { useState, useMemo } from 'react'
import { EdgeProps, getBezierPath, getEdgeCenter, useReactFlow } from 'reactflow'
import { EdgeInsertButton } from './EdgeInsertButton'
import { EdgeDestinationButton } from './EdgeDestinationButton'

export const CustomEdge: React.FC<EdgeProps> = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  source,
  target,
}) => {
  const [isHovered, setIsHovered] = useState(false)
  const { getEdges, getNodes } = useReactFlow()
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  })

  // Check if target node has no outgoing edges (terminal edge - can add destination)
  const canAddDestination = useMemo(() => {
    const edges = getEdges()
    const nodes = getNodes()
    const targetNode = nodes.find(n => n.id === target)
    
    // If target is already a destination, can't add another
    if (targetNode?.data?.type === 'destination') {
      return false
    }
    
    // Check if target has any outgoing edges
    const hasOutgoingEdges = edges.some(e => e.source === target)
    return !hasOutgoingEdges
  }, [target, getEdges, getNodes])

  const handleInsertClick = () => {
    console.log('[EDGE CLICK] Edge clicked:', { id, source, target })
    // Trigger edge insertion via custom event
    const event = new CustomEvent('edge-insert', {
      detail: { edgeId: id, sourceNodeId: source, targetNodeId: target }
    })
    console.log('[EDGE CLICK] Dispatching edge-insert event:', event.detail)
    window.dispatchEvent(event)
  }

  const handleAddDestinationClick = () => {
    console.log('[EDGE CLICK] Add destination clicked:', { id, source, target })
    // Trigger destination addition via custom event
    const event = new CustomEvent('edge-add-destination', {
      detail: { edgeId: id, sourceNodeId: source, targetNodeId: target }
    })
    console.log('[EDGE CLICK] Dispatching edge-add-destination event:', event.detail)
    window.dispatchEvent(event)
  }

  const handleContextMenu = (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    // Only show context menu for between-node insertion, not for destination
    if (!canAddDestination) {
      console.log('[EDGE CLICK] Edge right-clicked:', { id, source, target })
      handleInsertClick()
    }
  }

  return (
    <>
      {/* Invisible wider path for easier clicking */}
      <path
        id={`${id}-clickable`}
        style={{
          stroke: 'transparent',
          strokeWidth: 20, // Much wider for easier clicking
          cursor: 'pointer',
          pointerEvents: 'all',
        }}
        className="react-flow__edge-path"
        d={edgePath}
        onClick={handleInsertClick}
        onContextMenu={handleContextMenu}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      />
      {/* Visible edge path - use style.stroke from parent when set (e.g. green when source node completed) */}
      <path
        id={id}
        style={{
          ...style,
          stroke: isHovered ? '#818cf8' : (style?.stroke as string) || '#94a3b8',
          strokeWidth: isHovered ? 1.5 : (style?.strokeWidth as number) ?? 1,
          cursor: 'pointer',
          pointerEvents: 'none', // Let the invisible path handle clicks
        }}
        className="react-flow__edge-path"
        d={edgePath}
        markerEnd={markerEnd}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      />
      {isHovered && (
        <>
          {canAddDestination ? (
            <EdgeDestinationButton
              x={labelX}
              y={labelY}
              onAddDestination={handleAddDestinationClick}
            />
          ) : (
            <EdgeInsertButton
              x={labelX}
              y={labelY}
              onInsert={handleInsertClick}
            />
          )}
        </>
      )}
    </>
  )
}

export const edgeTypes = {
  default: CustomEdge,
}

