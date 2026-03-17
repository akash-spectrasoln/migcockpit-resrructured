from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .utils.compute_execution import ComputeNodeCompileView, ComputeNodeExecutionView
from .views import (
    AggregateXMLImportView,
    AggregateXMLValidateView,
    ColumnSequenceListView,
    ColumnSequenceView,
    # Expression views
    ColumnStatisticsView,
    CountryListView,
    CreateTableRecordView,
    CreateTableWithoutRecordsView,
    # User views
    CreateUserView,
    CustomerDestinationsView,
    CustomerSourcesView,
    DeleteTableRecordView,
    DeleteTableView,
    # Destination views
    DestinationConnectionCreateView,
    DestinationDeleteView,
    DestinationEditView,
    DestinationTablesView,
    DownloadTableDataView,
    EditTableRecordView,
    # Table views
    FileUploadPreviewView,
    # Pipeline views
    FilterExecutionView,
    GetDistinctValuesView,
    GetTableDataView,
    ImportDataFromHanaView,
    JoinExecutionView,
    ListUploadedTablesView,
    # Auth views
    LoginView,
    LogoutView,
    PipelineQueryExecutionView,
    PreviewTableDataView,
    # Project views
    ProjectsListView,
    RefreshTokenView,
    SourceAttributesView,
    SourceColumnsView,
    SourceConnectionCreateView,
    SourceConnectionCreateWithValidationView,
    SourceDeleteView,
    SourceEditView,
    SourceFieldsView,
    SourceLiveSchemaView,
    SourcesListView,
    SourceTableDataView,
    SourceTableSelectionView,
    SourceTablesView,
    # Source views
    SqlConnectionView,
    TruncateTableView,
    UpdateTableStructureView,
    UploadTableDataView,
    UserDeleteView,
    UserListView,
    UserPasswordResetConfirmView,
    UserPasswordResetView,
    UserUpdateView,
    # Misc views
    ValidationRulesView,
    WriteTableToDatabaseView,
)
from .views.canvas import CanvasViewSet
from .views.expressions import TestExpressionView, ValidateExpressionView
from .views.metadata import MetadataViewSet
from .views.migration import MigrationJobViewSet
from .views.nodes import (
    AddNodeAfterView,
    NodeCacheCleanupView,
    NodeCacheStatsView,
    NodeCacheView,
    NodeDeletionView,
    NodeInsertionView,
    PipelineRecomputeView,
)
from .views.projects import ProjectViewSet

# Router for ViewSets
router = DefaultRouter()
router.register(r'canvas', CanvasViewSet, basename='canvas')
router.register(r'migration-jobs', MigrationJobViewSet, basename='migration-job')
router.register(r'metadata', MetadataViewSet, basename='metadata')
router.register(r'projects', ProjectViewSet, basename='project')

urlpatterns = [
    # Authentication API endpoints
    path('api-login/', LoginView.as_view(), name='login-api'),
    path('api-logout/', LogoutView.as_view(), name='logout-api'),
    path('api-refresh/', RefreshTokenView.as_view(), name='refresh-api'),

    path('fetch/', SqlConnectionView.as_view()),
    path('sources/', SourcesListView.as_view()),
    path('sources/<int:source_id>/fields/', SourceFieldsView.as_view()),

    # Country API endpoints
    path('countries/', CountryListView.as_view(), name='country-list'),

    # Source API endpoints
    path('sources-connection/', SourceConnectionCreateView.as_view(), name='source-connection'),
    path('sources-connection-validate/', SourceConnectionCreateWithValidationView.as_view(), name='source-connection-validate'),
    path('source-attributes/', SourceAttributesView.as_view(), name='source-attributes'),
    path('source-attributes/<str:source_type>/', SourceAttributesView.as_view(), name='source-attributes-by-type'),

    # Destination API endpoints
    path('destinations-connection/', DestinationConnectionCreateView.as_view(), name='destination-connection'),

    # Customer sources API endpoint
    path('api-customer/sources/', CustomerSourcesView.as_view(), name='customer-sources-api'),

    # Source edit API endpoint
    path('api-customer/sources/<int:source_id>/edit/', SourceEditView.as_view(), name='source-edit-api'),

    # Source delete API endpoint
    path('api-customer/sources/<int:source_id>/delete/', SourceDeleteView.as_view(), name='source-delete-api'),

    # Source tables API endpoint (with pagination)
    path('api-customer/sources/<int:source_id>/tables/', SourceTablesView.as_view(), name='source-tables-api'),

    # Source table selection API endpoint
    path('api-customer/sources/<int:source_id>/selected-tables/', SourceTableSelectionView.as_view(), name='source-table-selection-api'),

    # Source table data API endpoint
    path('api-customer/sources/<int:source_id>/table-data/', SourceTableDataView.as_view(), name='source-table-data-api'),

    # Source columns API endpoint
    path('api-customer/sources/<int:source_id>/columns/', SourceColumnsView.as_view(), name='source-columns-api'),

    # Live schema for a single table (used for schema drift detection, no DB persistence)
    path('api-customer/sources/<int:source_id>/table/<str:table_name>/schema', SourceLiveSchemaView.as_view(), name='source-live-schema-api'),
    path('api-customer/sources/<int:source_id>/table/<str:table_name>/schema/', SourceLiveSchemaView.as_view(), name='source-live-schema-api-slash'),

    # Filter execution API endpoint
    path('api-customer/sources/<int:source_id>/filter/', FilterExecutionView.as_view(), name='filter-execution-api'),

    # Join execution API endpoint
    path('api-customer/sources/<int:source_id>/join/', JoinExecutionView.as_view(), name='join-execution-api'),

    # Pipeline query execution API endpoint
    path('pipeline/execute/', PipelineQueryExecutionView.as_view(), name='pipeline-query-execution-api'),
    # Node management API endpoints
    path('pipeline/insert-node/', NodeInsertionView.as_view(), name='pipeline-insert-node'),
    path('pipeline/insert-node', NodeInsertionView.as_view(), name='pipeline-insert-node-no-slash'),
    path('pipeline/add-node-after/', AddNodeAfterView.as_view(), name='pipeline-add-node-after'),
    path('pipeline/delete-node/', NodeDeletionView.as_view(), name='pipeline-delete-node'),
    path('pipeline/recompute/', PipelineRecomputeView.as_view(), name='pipeline-recompute'),
    path('validate-expression/', ValidateExpressionView.as_view(), name='validate-expression'),
    path('test-expression/', TestExpressionView.as_view(), name='test-expression'),

    # XML Query import and validation API endpoints
    path('xml-query/import/', AggregateXMLImportView.as_view(), name='xml-query-import-api'),
    path('xml-query/validate/', AggregateXMLValidateView.as_view(), name='xml-query-validate-api'),

    # Compute Node execution API endpoint
    path('compute/execute/', ComputeNodeExecutionView.as_view(), name='compute-execute-api'),
    # Compute Node compilation/validation API endpoint
    path('compute/compile/', ComputeNodeCompileView.as_view(), name='compute-compile-api'),

    # Node Cache API endpoints
    path('node-cache/<int:canvas_id>/<str:node_id>/', NodeCacheView.as_view(), name='node-cache-api'),
    path('node-cache/<int:canvas_id>/', NodeCacheView.as_view(), name='node-cache-canvas-api'),
    path('node-cache/stats/', NodeCacheStatsView.as_view(), name='node-cache-stats-api'),
    path('node-cache/stats/<int:canvas_id>/', NodeCacheStatsView.as_view(), name='node-cache-stats-canvas-api'),
    path('node-cache/cleanup/', NodeCacheCleanupView.as_view(), name='node-cache-cleanup-api'),

    # Customer destinations API endpoint
    path('api-customer/destinations/', CustomerDestinationsView.as_view(), name='customer-destinations-api'),

    # Destination edit API endpoint
    path('api-customer/destinations/<int:destination_id>/edit/', DestinationEditView.as_view(), name='destination-edit-api'),
    path('api-customer/destinations/<int:destination_id>/tables/', DestinationTablesView.as_view(), name='destination-tables-api'),

    # Destination delete API endpoint
    path('api-customer/destinations/<int:destination_id>/delete/', DestinationDeleteView.as_view(), name='destination-delete-api'),

    # File upload preview API endpoint
    path('api-file-upload-preview/', FileUploadPreviewView.as_view(), name='file-upload-preview-api'),

    # Write table to database API endpoint
    path('api-write-table/', WriteTableToDatabaseView.as_view(), name='write-table-api'),

    # Table management API endpoints
    path('api-list-uploaded-tables/<str:project_id>/', ListUploadedTablesView.as_view(), name='list-uploaded-tables-api'),
    path('api-get-table-data/', GetTableDataView.as_view(), name='get-table-data-api'),
    path('api-get-distinct-values/', GetDistinctValuesView.as_view(), name='get-distinct-values-api'),
    path('api-preview-table-data/', PreviewTableDataView.as_view(), name='preview-table-data-api'),
    path('api-upload-table-data/', UploadTableDataView.as_view(), name='upload-table-data-api'),

    # Table record management API endpoints
    path('api-create-table-record/', CreateTableRecordView.as_view(), name='create-table-record-api'),
    path('api-edit-table-record/', EditTableRecordView.as_view(), name='edit-table-record-api'),
    path('api-delete-table-record/', DeleteTableRecordView.as_view(), name='delete-table-record-api'),
    path('api-download-table-data/', DownloadTableDataView.as_view(), name='download-table-data-api'),

    # Table structure management API endpoint
    path('api-update-table-structure/', UpdateTableStructureView.as_view(), name='update-table-structure-api'),

    # Table deletion API endpoint
    path('api-delete-table/', DeleteTableView.as_view(), name='delete-table-api'),

    # Table creation API endpoint
    path('api-create-table/', CreateTableWithoutRecordsView.as_view(), name='create-table-api'),
    path('api-import-data-from-hana/', ImportDataFromHanaView.as_view(), name='import-data-from-hana-api'),
    path('api-truncate-table/', TruncateTableView.as_view()),
    path('api-create-user/', CreateUserView.as_view(), name='create-user-api'),
    path('api-list-users/', UserListView.as_view(), name='list-users-api'),
    path('api-update-user/<int:user_id>/', UserUpdateView.as_view(), name='update-user-api'),
    path('api-delete-user/<int:user_id>/', UserDeleteView.as_view(), name='delete-user-api'),
    path('api-reset-password/', UserPasswordResetView.as_view(), name='reset-password-api'),
    path('api-reset-password-confirm/', UserPasswordResetConfirmView.as_view(), name='reset-password-confirm-api'),
    path('api-projects-list/', ProjectsListView.as_view(), name='projects-list-api'),
    path('api-column-statistics/', ColumnStatisticsView.as_view(), name='api-column-statistics'),

    # Column sequence API endpoints
    # Column sequence API endpoints
    path('api-column-sequence-list/', ColumnSequenceListView.as_view(), name='column-sequence-list-api'),
    path('api-column-sequence/', ColumnSequenceView.as_view(), name='column-sequence-api'),
    path('api-validation-rules/', ValidationRulesView.as_view(), name='validation-rules-api'),

    # Canvas and Migration API endpoints (REST framework router)
    path('', include(router.urls)),
]

from .frontendviews import (
    create_table,
    create_user,
    customer_destinations,
    customer_sources,
    customer_user_dashboard,
    destination_connection_form,
    edit_destination,
    edit_source,
    edit_user,
    file_upload,
    import_data,
    login_page,
    password_reset_confirm,
    password_reset_request,
    projects_list,
    source_connection_form,
    sql_connection_form,
    table_data_display,
    table_management,
    table_navigation,
    user_delete,
    user_tables,
    users_list,
    validation_rules_page,
)

frontend_urlpatterns = [
    path('login/', login_page, name='login_page'),
    path('fetchsql', sql_connection_form, name='sql_connection_form'),
    path('add-source/', source_connection_form, name='source_connection_form'),
    path('add-destination/', destination_connection_form, name='destination_connection_form'),
    path('customer/sources/', customer_sources, name='customer_sources'),
    path('customer/sources/<int:source_id>/edit/', edit_source, name='edit_source'),
    path('customer/destinations/', customer_destinations, name='customer_destinations'),
    path('customer/destinations/<int:destination_id>/edit/', edit_destination, name='edit_destination'),
    path('file-upload/', file_upload, name='file_upload'),
    path('table-management/', table_management, name='table_management'),
    path('user-tables/<str:project_id>/', user_tables, name='user_added_tables'),
    path('table-data/<str:project_id>/', table_data_display, name='table_data_display'),
    path('create-table/', create_table, name='create_table'),
    path('import-data/', import_data, name='import_data'),
    path('<str:project_id>/table-navigation/', table_navigation, name='table_navigation'),
    path('customer-user-dashboard/', customer_user_dashboard, name='customer_user_dashboard'),
    path('users-list/', users_list, name='users_list'),
    path('create-user/', create_user, name='create_user'),
    path('user-update/<int:user_id>/', edit_user, name='user_update'),
    path('user-delete/<int:user_id>/', user_delete, name='user_delete'),
    path('projects-list/', projects_list, name='projects_list'),
    path('reset-password/', password_reset_request, name='reset-password-page'),
    path('reset-password-confirm/<str:uidb64>/<str:token>/', password_reset_confirm, name='reset-password-confirm-page'),
    path('validation-rules/', validation_rules_page, name='validation_rules_page'),
]

urlpatterns += frontend_urlpatterns
