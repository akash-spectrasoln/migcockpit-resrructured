"""
Port contract tests.

Verifies that:
1. All ports are proper ABCs (cannot be instantiated without implementing all methods)
2. A concrete mock implementation satisfies each port
3. The interface signatures match what the application expects

No database, no Django, no network.
Run with: python -m pytest tests/unit/ports/test_port_contracts.py -v
"""
import sys

sys.path.insert(0, '.')

from abc import ABC

from domain.connection.credential import Credential
from domain.job.migration_job import JobStatus, MigrationJob
from domain.pipeline.column import ColumnMetadata
from ports.cache_store import ICacheStore
from ports.encryption_service import IEncryptionService
from ports.job_repository import IJobRepository
from ports.pipeline_repository import IPipelineRepository
from ports.progress_notifier import IProgressNotifier
from ports.source_connector import ISourceConnector
from ports.tenant_repository import ITenantRepository

# ── All ports are ABCs ────────────────────────────────────────────────────────

def test_all_ports_are_abstract():
    for port in [ISourceConnector, IPipelineRepository, IJobRepository,
                 ICacheStore, IProgressNotifier, IEncryptionService, ITenantRepository]:
        assert issubclass(port, ABC), f"{port.__name__} should be an ABC"


def test_cannot_instantiate_source_connector_directly():
    try:
        ISourceConnector()
        assert False, "Should not be able to instantiate abstract class"
    except TypeError:
        pass


def test_cannot_instantiate_pipeline_repository_directly():
    try:
        IPipelineRepository()
        assert False, "Should not be able to instantiate abstract class"
    except TypeError:
        pass


def test_cannot_instantiate_cache_store_directly():
    try:
        ICacheStore()
        assert False, "Should not be able to instantiate abstract class"
    except TypeError:
        pass


# ── Concrete implementations satisfy each port ────────────────────────────────

class MockSourceConnector(ISourceConnector):
    def test_connection(self, credential):
        return {'success': True, 'message': 'ok'}
    def fetch_tables(self, credential, schema):
        return ['users', 'orders']
    def fetch_schema(self, credential, table, schema):
        return [ColumnMetadata(name='id', technical_name='id', business_name='ID', datatype='INTEGER')]
    def execute_query(self, credential, sql, params):
        return {'columns': ['id'], 'rows': [[1]], 'row_count': 1}


class MockPipelineRepository(IPipelineRepository):
    def load_canvas(self, canvas_id):
        return {'nodes': [], 'edges': []}
    def save_canvas(self, canvas_id, nodes, edges):
        pass
    def load_node_config(self, node_id):
        return {}
    def save_node_output_metadata(self, node_id, metadata):
        pass


class MockJobRepository(IJobRepository):
    def create(self, job):
        return job
    def update_status(self, job_id, status, progress=0.0, current_step='', error_message=''):
        pass
    def get_by_id(self, job_id):
        return None
    def get_logs(self, job_id):
        return []


class MockCacheStore(ICacheStore):
    def __init__(self):
        self._store = {}
    def get(self, key):
        return self._store.get(key)
    def set(self, key, value, ttl_seconds=3600):
        self._store[key] = value
    def invalidate(self, key):
        self._store.pop(key, None)
    def invalidate_canvas(self, canvas_id):
        keys = [k for k in self._store if str(canvas_id) in k]
        for k in keys:
            del self._store[k]


class MockProgressNotifier(IProgressNotifier):
    def __init__(self):
        self.events = []
    def emit_progress(self, job_id, step, percent):
        self.events.append(('progress', job_id, step, percent))
    def emit_node_complete(self, job_id, node_id, row_count):
        self.events.append(('node_complete', job_id, node_id, row_count))
    def emit_error(self, job_id, error_message):
        self.events.append(('error', job_id, error_message))
    def emit_complete(self, job_id, stats):
        self.events.append(('complete', job_id, stats))


class MockEncryptionService(IEncryptionService):
    def encrypt(self, plaintext, customer_id, created_on):
        return f"encrypted:{plaintext}"
    def decrypt(self, encrypted_blob, customer_id, created_on):
        return encrypted_blob.replace("encrypted:", "")


def test_mock_source_connector_satisfies_interface():
    connector = MockSourceConnector()
    cred = Credential(host='h', port=5432, username='u', password='p', database='d', db_type='postgresql')
    result = connector.test_connection(cred)
    assert result['success'] is True


def test_mock_source_connector_fetch_tables():
    connector = MockSourceConnector()
    cred = Credential(host='h', port=5432, username='u', password='p', database='d', db_type='postgresql')
    tables = connector.fetch_tables(cred, 'public')
    assert isinstance(tables, list)
    assert len(tables) > 0


def test_mock_pipeline_repository_roundtrip():
    repo = MockPipelineRepository()
    repo.save_canvas(1, [], [])
    result = repo.load_canvas(1)
    assert 'nodes' in result
    assert 'edges' in result


def test_mock_cache_store_get_set_invalidate():
    cache = MockCacheStore()
    cache.set('key1', {'data': 123})
    assert cache.get('key1') == {'data': 123}
    cache.invalidate('key1')
    assert cache.get('key1') is None


def test_mock_cache_store_invalidate_canvas():
    cache = MockCacheStore()
    cache.set('canvas_5_node_a', 'data_a')
    cache.set('canvas_5_node_b', 'data_b')
    cache.set('canvas_9_node_c', 'data_c')
    cache.invalidate_canvas(5)
    assert cache.get('canvas_5_node_a') is None
    assert cache.get('canvas_5_node_b') is None
    assert cache.get('canvas_9_node_c') == 'data_c'  # different canvas — unaffected


def test_mock_progress_notifier_records_events():
    notifier = MockProgressNotifier()
    notifier.emit_progress('job1', 'Extracting', 25.0)
    notifier.emit_node_complete('job1', 'node_a', 1000)
    notifier.emit_complete('job1', {'rows': 1000})
    assert len(notifier.events) == 3
    assert notifier.events[0][0] == 'progress'
    assert notifier.events[1][0] == 'node_complete'
    assert notifier.events[2][0] == 'complete'


def test_mock_encryption_service_roundtrip():
    svc = MockEncryptionService()
    original = "super_secret_password"
    encrypted = svc.encrypt(original, 'C00001', None)
    decrypted = svc.decrypt(encrypted, 'C00001', None)
    assert decrypted == original


def test_mock_job_repository_create_returns_job():
    repo = MockJobRepository()
    job = MigrationJob(job_id='j001', canvas_id=1, customer_id='C00001')
    result = repo.create(job)
    assert result.job_id == 'j001'


# ── ISourceConnector must reject partial implementations ──────────────────────

def test_partial_source_connector_cannot_instantiate():
    class PartialConnector(ISourceConnector):
        def test_connection(self, credential):
            return {}
        # Missing: fetch_tables, fetch_schema, execute_query

    try:
        PartialConnector()
        assert False, "Partial implementation should not instantiate"
    except TypeError:
        pass


# ── Port method signatures ─────────────────────────────────────────────────────

def test_source_connector_has_required_methods():
    required = ['test_connection', 'fetch_tables', 'fetch_schema', 'execute_query']
    for method in required:
        assert hasattr(ISourceConnector, method), f"ISourceConnector missing method: {method}"


def test_pipeline_repository_has_required_methods():
    required = ['load_canvas', 'save_canvas', 'load_node_config', 'save_node_output_metadata']
    for method in required:
        assert hasattr(IPipelineRepository, method), f"IPipelineRepository missing method: {method}"


def test_cache_store_has_required_methods():
    required = ['get', 'set', 'invalidate', 'invalidate_canvas']
    for method in required:
        assert hasattr(ICacheStore, method), f"ICacheStore missing method: {method}"


def test_progress_notifier_has_required_methods():
    required = ['emit_progress', 'emit_node_complete', 'emit_error', 'emit_complete']
    for method in required:
        assert hasattr(IProgressNotifier, method), f"IProgressNotifier missing method: {method}"
