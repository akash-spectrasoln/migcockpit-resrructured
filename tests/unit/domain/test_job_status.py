"""
Unit tests for JobStatus state machine.
No database or Django required.
"""
from domain.job.migration_job import JobStatus, MigrationJob


def test_new_migration_job_defaults_to_pending():
    job = MigrationJob(job_id='job_001', canvas_id=1, customer_id='C00001')
    assert job.status == JobStatus.PENDING
    assert job.progress == 0.0
    assert job.current_step == ''


def test_job_status_terminal_states():
    assert JobStatus.COMPLETED.is_terminal() is True
    assert JobStatus.FAILED.is_terminal() is True
    assert JobStatus.CANCELLED.is_terminal() is True
    assert JobStatus.PENDING.is_terminal() is False
    assert JobStatus.RUNNING.is_terminal() is False


def test_valid_status_transitions():
    assert JobStatus.PENDING.can_transition_to(JobStatus.RUNNING) is True
    assert JobStatus.RUNNING.can_transition_to(JobStatus.COMPLETED) is True
    assert JobStatus.RUNNING.can_transition_to(JobStatus.FAILED) is True


def test_invalid_status_transitions():
    assert JobStatus.COMPLETED.can_transition_to(JobStatus.RUNNING) is False
    assert JobStatus.FAILED.can_transition_to(JobStatus.RUNNING) is False
    assert JobStatus.PENDING.can_transition_to(JobStatus.COMPLETED) is False


def test_node_type_sql_compilable():
    from domain.pipeline.node import Node, NodeType
    source = Node(id='n1', node_type=NodeType.SOURCE, config={})
    compute = Node(id='n2', node_type=NodeType.COMPUTE, config={})
    assert source.is_sql_compilable() is True
    assert compute.is_sql_compilable() is False
