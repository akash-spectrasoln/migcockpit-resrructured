"""
Test Validation-Gated Execution Lifecycle
Demonstrates strict state management and execution guards.
"""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _root not in sys.path:
    sys.path.insert(0, _root)

from services.migration_service.lifecycle import (
    PipelineState,
    can_execute,
    execute_validated_plan,
    get_pipeline_state,
    invalidate_validation,
    validate_pipeline,
)
from services.migration_service.lifecycle.validated_plan_storage import InMemoryPipelineStorage


class MockExecutor:
    """Mock executor for testing."""

    def __init__(self):
        self.executed_plans = []

    def execute_plan(self, execution_plan):
        """Record executed plan."""
        self.executed_plans.append(execution_plan)
        return {
            "status": "success",
            "rows_inserted": 1000,
            "duration_seconds": 15.5
        }


def test_lifecycle_happy_path():
    """Test: Happy path - validate → execute → success."""
    print("\n" + "="*80)
    print("TEST 1: Happy Path (Validate → Execute → Success)")
    print("="*80)

    storage = InMemoryPipelineStorage()
    executor = MockExecutor()
    job_id = "test_job_001"

    # Simple pipeline
    nodes = [
        {"id": "source_1", "type": "source", "data": {"config": {"tableName": "sales"}}},
        {"id": "proj_1", "type": "projection", "data": {"config": {}}},
        {"id": "dest_1", "type": "destination", "data": {"config": {"tableName": "analytics"}}}
    ]
    edges = [
        {"source": "source_1", "target": "proj_1"},
        {"source": "proj_1", "target": "dest_1"}
    ]
    config = {"source_configs": {}, "destination_configs": {}}

    # Step 1: Initial state should be DRAFT
    print("\n1. Check initial state:")
    metadata = get_pipeline_state(job_id, storage)
    print(f"   State: {metadata.state.value}")
    print(f"   Plan exists: {metadata.execution_plan_json is not None}")
    assert metadata.state == PipelineState.DRAFT
    assert metadata.execution_plan_json is None
    print("   ✓ Initial state is DRAFT")

    # Step 2: Execution should be blocked
    print("\n2. Try to execute without validation:")
    can_exec, error = can_execute(job_id, nodes, edges, storage)
    print(f"   Can execute: {can_exec}")
    print(f"   Error: {error}")
    assert can_exec is False
    print("   ✓ Execution blocked (no validation)")

    # Step 3: Validate pipeline
    print("\n3. Validate pipeline:")
    result = validate_pipeline(job_id, nodes, edges, config, storage)
    print(f"   Valid: {result.is_valid}")
    print(f"   Plan hash: {result.plan_hash[:16]}...")
    print(f"   Validated at: {result.validated_at}")
    assert result.is_valid is True
    assert result.plan_hash is not None
    print("   ✓ Validation successful")

    # Step 4: Check state after validation
    print("\n4. Check state after validation:")
    metadata = get_pipeline_state(job_id, storage)
    print(f"   State: {metadata.state.value}")
    print(f"   Plan exists: {metadata.execution_plan_json is not None}")
    assert metadata.state == PipelineState.VALIDATED
    assert metadata.execution_plan_json is not None
    print("   ✓ State is VALIDATED, plan exists")

    # Step 5: Execution should now be allowed
    print("\n5. Check if execution is allowed:")
    can_exec, error = can_execute(job_id, nodes, edges, storage)
    print(f"   Can execute: {can_exec}")
    assert can_exec is True
    print("   ✓ Execution allowed")

    # Step 6: Execute pipeline
    print("\n6. Execute pipeline:")
    exec_result = execute_validated_plan(job_id, storage, executor)
    print(f"   Status: {exec_result['status']}")
    print(f"   Rows inserted: {exec_result['rows_inserted']}")
    print(f"   Duration: {exec_result['duration_seconds']}s")
    assert exec_result["status"] == "success"
    print("   ✓ Execution successful")

    # Step 7: Check final state
    print("\n7. Check final state:")
    metadata = get_pipeline_state(job_id, storage)
    print(f"   State: {metadata.state.value}")
    assert metadata.state == PipelineState.SUCCESS
    print("   ✓ State is SUCCESS")

    print("\n" + "="*80)
    print("TEST 1 PASSED ✓")
    print("="*80)


def test_dag_mutation_invalidates_validation():
    """Test: DAG mutation invalidates validation."""
    print("\n" + "="*80)
    print("TEST 2: DAG Mutation Invalidates Validation")
    print("="*80)

    storage = InMemoryPipelineStorage()
    job_id = "test_job_002"

    nodes = [
        {"id": "source_1", "type": "source", "data": {"config": {}}},
        {"id": "dest_1", "type": "destination", "data": {"config": {}}}
    ]
    edges = [{"source": "source_1", "target": "dest_1"}]
    config = {}

    # Step 1: Validate pipeline
    print("\n1. Validate pipeline:")
    result = validate_pipeline(job_id, nodes, edges, config, storage)
    print(f"   Valid: {result.is_valid}")
    print(f"   State: {get_pipeline_state(job_id, storage).state.value}")
    assert result.is_valid is True
    assert get_pipeline_state(job_id, storage).state == PipelineState.VALIDATED
    print("   ✓ Pipeline validated")

    # Step 2: Simulate DAG mutation (user adds a node)
    print("\n2. Simulate DAG mutation (add node):")
    print("   User adds a projection node...")
    invalidate_validation(job_id, storage)
    print("   invalidate_validation() called")

    # Step 3: Check state after mutation
    print("\n3. Check state after mutation:")
    metadata = get_pipeline_state(job_id, storage)
    print(f"   State: {metadata.state.value}")
    print(f"   Plan exists: {metadata.execution_plan_json is not None}")
    print(f"   Plan hash: {metadata.plan_hash}")
    assert metadata.state == PipelineState.DRAFT
    assert metadata.execution_plan_json is None
    assert metadata.plan_hash is None
    print("   ✓ Validation invalidated, state is DRAFT")

    # Step 4: Execution should be blocked
    print("\n4. Try to execute after mutation:")
    nodes_mutated = [*nodes, {"id": "proj_1", "type": "projection", "data": {}}]
    can_exec, error = can_execute(job_id, nodes_mutated, edges, storage)
    print(f"   Can execute: {can_exec}")
    print(f"   Error: {error}")
    assert can_exec is False
    print("   ✓ Execution blocked (validation invalidated)")

    print("\n" + "="*80)
    print("TEST 2 PASSED ✓")
    print("="*80)


def test_execution_uses_stored_plan():
    """Test: Execution uses stored plan, not rebuilt."""
    print("\n" + "="*80)
    print("TEST 3: Execution Uses Stored Plan (Not Rebuilt)")
    print("="*80)

    storage = InMemoryPipelineStorage()
    executor = MockExecutor()
    job_id = "test_job_003"

    nodes = [
        {"id": "source_1", "type": "source", "data": {"config": {}}},
        {"id": "dest_1", "type": "destination", "data": {"config": {}}}
    ]
    edges = [{"source": "source_1", "target": "dest_1"}]
    config = {}

    # Step 1: Validate
    print("\n1. Validate pipeline:")
    result = validate_pipeline(job_id, nodes, edges, config, storage)
    original_plan_hash = result.plan_hash
    print(f"   Plan hash: {original_plan_hash[:16]}...")
    print("   ✓ Validated")

    # Step 2: Execute
    print("\n2. Execute pipeline:")
    execute_validated_plan(job_id, storage, executor)
    print("   ✓ Executed")

    # Step 3: Verify executor received stored plan
    print("\n3. Verify executor used stored plan:")
    assert len(executor.executed_plans) == 1
    executed_plan = executor.executed_plans[0]
    print(f"   Executor received plan with job_id: {executed_plan['job_id']}")
    print(f"   Plan has {len(executed_plan['levels'])} levels")
    print(f"   Plan has {executed_plan['total_queries']} queries")

    # Verify the executed plan matches the stored plan
    metadata = get_pipeline_state(job_id, storage)
    import json
    stored_plan = json.loads(metadata.execution_plan_json)
    assert executed_plan == stored_plan
    print("   ✓ Executor used stored plan (NOT rebuilt)")

    print("\n" + "="*80)
    print("TEST 3 PASSED ✓")
    print("="*80)


def test_hash_mismatch_blocks_execution():
    """Test: Hash mismatch blocks execution."""
    print("\n" + "="*80)
    print("TEST 4: Hash Mismatch Blocks Execution")
    print("="*80)

    storage = InMemoryPipelineStorage()
    job_id = "test_job_004"

    nodes = [
        {"id": "source_1", "type": "source", "data": {"config": {}}},
        {"id": "dest_1", "type": "destination", "data": {"config": {}}}
    ]
    edges = [{"source": "source_1", "target": "dest_1"}]
    config = {}

    # Step 1: Validate
    print("\n1. Validate pipeline:")
    result = validate_pipeline(job_id, nodes, edges, config, storage)
    print(f"   Original hash: {result.plan_hash[:16]}...")
    print("   ✓ Validated")

    # Step 2: Corrupt plan hash (simulate data corruption)
    print("\n2. Simulate data corruption (corrupt hash):")
    storage._storage[job_id]["plan_hash"] = "corrupted_hash_12345"
    print("   Plan hash corrupted")

    # Step 3: Try to execute
    print("\n3. Try to execute with corrupted hash:")
    can_exec, error = can_execute(job_id, nodes, edges, storage)
    print(f"   Can execute: {can_exec}")
    print(f"   Error: {error}")
    assert can_exec is False
    assert "hash mismatch" in error.lower()
    print("   ✓ Execution blocked (hash mismatch detected)")

    print("\n" + "="*80)
    print("TEST 4 PASSED ✓")
    print("="*80)


if __name__ == "__main__":
    print("\n" + "="*80)
    print("VALIDATION-GATED EXECUTION LIFECYCLE - TEST SUITE")
    print("="*80)

    test_lifecycle_happy_path()
    test_dag_mutation_invalidates_validation()
    test_execution_uses_stored_plan()
    test_hash_mismatch_blocks_execution()

    print("\n" + "="*80)
    print("ALL TESTS PASSED ✓")
    print("="*80)
    print("\nKey Achievements:")
    print("✓ Execution blocked without validation")
    print("✓ DAG mutation invalidates validation")
    print("✓ Execution uses stored plan (not rebuilt)")
    print("✓ Hash mismatch blocks execution")
    print("✓ Strict state machine enforced")
    print("✓ Production-safe orchestration")
    print("\n")
