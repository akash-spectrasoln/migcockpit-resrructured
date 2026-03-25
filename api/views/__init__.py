"""
Import all views
This module re-exports views from organized submodules (auth.py, users.py, sources.py, etc.)
All views have been extracted from the monolithic views.py into domain-specific modules.
"""

# Import from submodules
from .auth import LoginView, LogoutView, RefreshTokenView
from .canvas import CanvasViewSet
from .destinations import (
    CustomerDestinationsView,
    DestinationConnectionCreateView,
    DestinationDeleteView,
    DestinationEditView,
    DestinationTablesView,
)

# Expression views
from .expressions import ColumnSequenceListView, ColumnSequenceView, ColumnStatisticsView, FilterColumnValuesView
from .migration import MigrationJobViewSet

# Pipeline views
from .pipeline import FilterExecutionView, JoinExecutionView, PipelineQueryExecutionView
from .projects import ProjectsListView
from .query_parser import AggregateXMLImportView, AggregateXMLValidateView
from .sources import (
    CountryListView,
    CustomerSourcesView,
    SourceAttributesView,
    SourceColumnsView,
    SourceConnectionCreateView,
    SourceConnectionCreateWithValidationView,
    SourceDeleteView,
    SourceEditView,
    SourceFieldsView,
    SourceLiveSchemaView,
    RepositorySchemaTablesView,
    RepositoryColumnsView,
    RepositoryTableDataView,
    RepositoryFilterExecutionView,
    SourcesListView,
    SourceTableDataView,
    SourceTableSelectionView,
    SourceTablesView,
    SqlConnectionView,
)

# Table views
from .tables import (
    CreateTableRecordView,
    CreateTableWithoutRecordsView,
    DeleteTableRecordView,
    DeleteTableView,
    DownloadTableDataView,
    EditTableRecordView,
    FileUploadPreviewView,
    GetDistinctValuesView,
    GetTableDataView,
    ImportDataFromHanaView,
    ListUploadedTablesView,
    PreviewTableDataView,
    TruncateTableView,
    UpdateTableStructureView,
    UploadTableDataView,
    WriteTableToDatabaseView,
)
from .users import (
    CreateUserView,
    UserDeleteView,
    UserListView,
    UserPasswordResetConfirmView,
    UserPasswordResetView,
    UserUpdateView,
)
from .utils import ValidationRulesView

__all__ = [
    # Auth views
    'LoginView', 'LogoutView', 'RefreshTokenView',
    # User views
    'CreateUserView', 'UserListView', 'UserUpdateView', 'UserDeleteView',
    'UserPasswordResetView', 'UserPasswordResetConfirmView',
    # Source views
    'SqlConnectionView', 'SourcesListView', 'SourceFieldsView', 'CountryListView',
    'SourceConnectionCreateView', 'CustomerSourcesView', 'SourceAttributesView',
    'SourceConnectionCreateWithValidationView', 'SourceEditView', 'SourceDeleteView',
    'SourceTablesView', 'SourceTableDataView', 'SourceColumnsView', 'SourceTableSelectionView',
    'SourceLiveSchemaView', 'RepositorySchemaTablesView', 'RepositoryColumnsView', 'RepositoryTableDataView', 'RepositoryFilterExecutionView',
    # Destination views
    'DestinationConnectionCreateView', 'CustomerDestinationsView',
    'DestinationEditView', 'DestinationDeleteView', 'DestinationTablesView',
    # Project views
    'ProjectsListView',
    # Misc views
    'AggregateXMLImportView', 'AggregateXMLValidateView',
    # Pipeline views
    'FilterExecutionView', 'JoinExecutionView', 'PipelineQueryExecutionView',
    # Expression views
    'FilterColumnValuesView', 'ColumnStatisticsView', 'ColumnSequenceListView', 'ColumnSequenceView',
    # Table views
    'FileUploadPreviewView', 'WriteTableToDatabaseView', 'ListUploadedTablesView',
    'GetTableDataView', 'GetDistinctValuesView', 'PreviewTableDataView', 'UploadTableDataView',
    'CreateTableRecordView', 'EditTableRecordView', 'DeleteTableRecordView',
    'UpdateTableStructureView', 'DeleteTableView', 'CreateTableWithoutRecordsView',
    'ImportDataFromHanaView', 'DownloadTableDataView', 'TruncateTableView',
    # Utility views
    'ValidationRulesView',
    # Canvas views
    'CanvasViewSet',
    # Migration views
    'MigrationJobViewSet'
]
