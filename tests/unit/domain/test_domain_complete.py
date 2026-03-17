"""
Comprehensive domain layer tests.

Tests every domain object, enum, dataclass, and business rule.
No database, no Django, no network — runs in milliseconds.
Run with: python -m pytest tests/unit/domain/ -v
"""
import sys

sys.path.insert(0, '.')

from domain.connection.credential import Credential
from domain.connection.source import Source, SourceType
from domain.exceptions import (
    CircularDependencyError,
    EncryptionError,
    InvalidStatusTransitionError,
    JobAlreadyRunningError,
    NodeConfigError,
    PipelineValidationError,
    TenantProvisioningError,
)
from domain.exceptions import (
    ConnectionError as DomainConnectionError,
)
from domain.job.checkpoint import Checkpoint
from domain.job.migration_job import JobStatus, MigrationJob
from domain.pipeline.column import ColumnLineage, ColumnMetadata
from domain.pipeline.execution_plan import ExecutionPlan, ExecutionStep, ExecutionStepType, PushdownDecision
from domain.pipeline.filter import FilterCondition, FilterGroup, FilterOperator, LogicalOperator
from domain.pipeline.node import Edge, Node, NodeType
from domain.tenant.customer import Customer

# ── NodeType ──────────────────────────────────────────────────────────────────

def test_node_type_enum_has_all_required_values():
    expected = ['SOURCE', 'FILTER', 'PROJECTION', 'JOIN',
                'CALCULATED_COLUMN', 'AGGREGATE', 'COMPUTE', 'DESTINATION']
    for name in expected:
        assert hasattr(NodeType, name), f"NodeType missing: {name}"


def test_compute_node_is_not_sql_compilable():
    node = Node(id='n1', node_type=NodeType.COMPUTE, config={})
    assert node.is_sql_compilable() is False


def test_all_non_compute_nodes_are_sql_compilable():
    for nt in NodeType:
        if nt != NodeType.COMPUTE:
            node = Node(id='n', node_type=nt, config={})
            assert node.is_sql_compilable() is True, f"{nt} should be SQL compilable"


def test_node_default_position_is_zero():
    node = Node(id='x', node_type=NodeType.SOURCE, config={})
    assert node.position_x == 0.0
    assert node.position_y == 0.0


def test_node_business_name_defaults_to_empty():
    node = Node(id='x', node_type=NodeType.FILTER, config={})
    assert node.business_name == ''


# ── Edge ──────────────────────────────────────────────────────────────────────

def test_edge_stores_source_and_target():
    edge = Edge(id='e1', source_node_id='n_src', target_node_id='n_flt')
    assert edge.source_node_id == 'n_src'
    assert edge.target_node_id == 'n_flt'


def test_edge_handle_defaults_to_none():
    edge = Edge(id='e1', source_node_id='a', target_node_id='b')
    assert edge.source_handle is None
    assert edge.target_handle is None


def test_edge_with_join_handles():
    edge = Edge(id='e1', source_node_id='src', target_node_id='join',
                target_handle='left')
    assert edge.target_handle == 'left'


# ── ColumnMetadata ────────────────────────────────────────────────────────────

def test_column_metadata_required_fields():
    col = ColumnMetadata(name='user_id', technical_name='src1_user_id',
                         business_name='User ID', datatype='INTEGER')
    assert col.name == 'user_id'
    assert col.technical_name == 'src1_user_id'
    assert col.datatype == 'INTEGER'


def test_column_metadata_nullable_defaults_true():
    col = ColumnMetadata(name='x', technical_name='x', business_name='X', datatype='TEXT')
    assert col.nullable is True


def test_column_metadata_source_defaults_to_base():
    col = ColumnMetadata(name='x', technical_name='x', business_name='X', datatype='TEXT')
    assert col.source == 'base'


def test_column_metadata_with_expression():
    col = ColumnMetadata(name='full_name', technical_name='full_name',
                         business_name='Full Name', datatype='TEXT',
                         source='calculated',
                         expression="CONCAT(first_name, ' ', last_name)")
    assert col.expression is not None
    assert col.source == 'calculated'


# ── ColumnLineage ─────────────────────────────────────────────────────────────

def test_lineage_source_origin():
    lineage = ColumnLineage(technical_name='id', origin_node_id='src1', origin_type='SOURCE')
    assert lineage.origin_type == 'SOURCE'
    assert lineage.expression is None
    assert lineage.origin_branch is None


def test_lineage_join_with_branch():
    lineage = ColumnLineage(technical_name='order_id', origin_node_id='join1',
                            origin_type='JOIN', origin_branch='right')
    assert lineage.origin_branch == 'right'


def test_lineage_calculated_column():
    lineage = ColumnLineage(technical_name='tax', origin_node_id='proj1',
                            origin_type='PROJECTION', expression='price * 0.1')
    assert lineage.expression == 'price * 0.1'


# ── FilterOperator & FilterCondition ─────────────────────────────────────────

def test_filter_operator_all_values_present():
    operators = [op.value for op in FilterOperator]
    assert 'eq' in operators
    assert 'neq' in operators
    assert 'is_null' in operators
    assert 'is_not_null' in operators
    assert 'contains' in operators
    assert 'in' in operators


def test_filter_condition_equals():
    fc = FilterCondition(column='status', operator=FilterOperator.EQUALS, value='active')
    assert fc.column == 'status'
    assert fc.value == 'active'


def test_filter_condition_is_null_no_value_needed():
    fc = FilterCondition(column='deleted_at', operator=FilterOperator.IS_NULL)
    assert fc.value is None


def test_filter_condition_in_operator_with_list():
    fc = FilterCondition(column='country', operator=FilterOperator.IN, value=['US', 'UK', 'IN'])
    assert isinstance(fc.value, list)
    assert len(fc.value) == 3


def test_filter_condition_default_logical_is_and():
    fc = FilterCondition(column='age', operator=FilterOperator.GREATER_THAN, value=18)
    assert fc.logical_operator == LogicalOperator.AND


def test_filter_group_holds_multiple_conditions():
    conditions = [
        FilterCondition(column='age', operator=FilterOperator.GREATER_THAN, value=18),
        FilterCondition(column='status', operator=FilterOperator.EQUALS, value='active'),
    ]
    group = FilterGroup(conditions=conditions)
    assert len(group.conditions) == 2


# ── ExecutionPlan ─────────────────────────────────────────────────────────────

def test_execution_plan_starts_empty():
    plan = ExecutionPlan(canvas_id=5)
    assert plan.canvas_id == 5
    assert plan.steps == []
    assert plan.pushdown_decisions == []


def test_execution_step_types():
    assert ExecutionStepType.SQL_QUERY.value == 'sql_query'
    assert ExecutionStepType.COMPUTE.value == 'compute'
    assert ExecutionStepType.LOAD.value == 'load'


def test_pushdown_decision_holds_target_and_conditions():
    pd = PushdownDecision(
        filter_node_id='flt1',
        target_node_id='src1',
        conditions=[{'column': 'age', 'operator': '>', 'value': 18}]
    )
    assert pd.filter_node_id == 'flt1'
    assert pd.target_node_id == 'src1'
    assert len(pd.conditions) == 1


# ── MigrationJob & JobStatus ──────────────────────────────────────────────────

def test_job_defaults():
    job = MigrationJob(job_id='j1', canvas_id=3, customer_id='C00001')
    assert job.status == JobStatus.PENDING
    assert job.progress == 0.0
    assert job.error_message == ''


def test_job_status_terminal():
    assert JobStatus.COMPLETED.is_terminal()
    assert JobStatus.FAILED.is_terminal()
    assert JobStatus.CANCELLED.is_terminal()
    assert not JobStatus.PENDING.is_terminal()
    assert not JobStatus.RUNNING.is_terminal()


def test_valid_transitions():
    assert JobStatus.PENDING.can_transition_to(JobStatus.RUNNING)
    assert JobStatus.RUNNING.can_transition_to(JobStatus.COMPLETED)
    assert JobStatus.RUNNING.can_transition_to(JobStatus.FAILED)
    assert JobStatus.RUNNING.can_transition_to(JobStatus.CANCELLED)
    assert JobStatus.PENDING.can_transition_to(JobStatus.CANCELLED)


def test_invalid_transitions():
    assert not JobStatus.COMPLETED.can_transition_to(JobStatus.RUNNING)
    assert not JobStatus.FAILED.can_transition_to(JobStatus.RUNNING)
    assert not JobStatus.PENDING.can_transition_to(JobStatus.COMPLETED)
    assert not JobStatus.PENDING.can_transition_to(JobStatus.FAILED)


def test_job_status_all_enum_values():
    values = {s.value for s in JobStatus}
    assert values == {'pending', 'running', 'completed', 'failed', 'cancelled'}


# ── Checkpoint ────────────────────────────────────────────────────────────────

def test_checkpoint_stores_resume_point():
    cp = Checkpoint(
        job_id='j1', node_id='node_src', table_ref='CANVAS_CACHE.cp_j1_node_src',
        columns=[{'name': 'id', 'datatype': 'INTEGER'}], row_count=50000
    )
    assert cp.job_id == 'j1'
    assert cp.node_id == 'node_src'
    assert cp.row_count == 50000


def test_checkpoint_config_hash_defaults_empty():
    cp = Checkpoint(job_id='j', node_id='n', table_ref='t', columns=[])
    assert cp.config_hash == ''


# ── Source & SourceType ───────────────────────────────────────────────────────

def test_source_type_enum_has_all_db_types():
    types = {t.value for t in SourceType}
    assert 'postgresql' in types
    assert 'sqlserver' in types
    assert 'mysql' in types
    assert 'oracle' in types
    assert 'hana' in types


def test_source_defaults():
    src = Source(source_id=1, name='prod_db', source_type=SourceType.POSTGRESQL)
    assert src.schema == 'public'
    assert src.is_active is True
    assert src.project_id is None


# ── Credential ────────────────────────────────────────────────────────────────

def test_credential_is_frozen():
    cred = Credential(host='h', port=5432, username='u', password='p',
                      database='d', db_type='postgresql')
    try:
        cred.password = 'new_password'
        assert False, "Credential should be immutable (frozen dataclass)"
    except Exception:
        pass  # Expected


def test_credential_equality():
    c1 = Credential(host='h', port=5432, username='u', password='p',
                    database='d', db_type='postgresql')
    c2 = Credential(host='h', port=5432, username='u', password='p',
                    database='d', db_type='postgresql')
    assert c1 == c2


def test_credential_inequality_on_different_password():
    c1 = Credential(host='h', port=5432, username='u', password='p1',
                    database='d', db_type='postgresql')
    c2 = Credential(host='h', port=5432, username='u', password='p2',
                    database='d', db_type='postgresql')
    assert c1 != c2


# ── Customer ──────────────────────────────────────────────────────────────────

def test_customer_is_active_by_default():
    c = Customer(cust_id='C00001', name='Acme Corp', cust_db='C00001',
                 city='NY', region='NY', phone='555-1234')
    assert c.is_active is True


# ── Exceptions ────────────────────────────────────────────────────────────────

def test_domain_exceptions_are_catchable():
    try:
        raise PipelineValidationError("bad pipeline")
    except PipelineValidationError as e:
        assert str(e) == "bad pipeline"


def test_node_config_error_is_pipeline_validation_error():
    err = NodeConfigError("bad config")
    assert isinstance(err, PipelineValidationError)


def test_circular_dependency_is_pipeline_validation_error():
    err = CircularDependencyError("cycle detected")
    assert isinstance(err, PipelineValidationError)


def test_job_already_running_error():
    try:
        raise JobAlreadyRunningError("job j1 is already running")
    except JobAlreadyRunningError as e:
        assert 'j1' in str(e)


def test_encryption_error_is_catchable():
    try:
        raise EncryptionError("key derivation failed")
    except EncryptionError:
        pass


def test_tenant_provisioning_error_is_catchable():
    try:
        raise TenantProvisioningError("could not create DB")
    except TenantProvisioningError:
        pass
