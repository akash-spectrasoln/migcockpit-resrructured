"""
Serializers module
This module re-exports serializers from base_serializers.py and submodules
"""

# Import legacy serializers from base_serializers
from .base_serializers import (
    CountrySerializer,
    CustomerSerializer,
    DestinationConnectionSerializer,
    FileUploadSerializer,
    SourceConnectionSerializer,
    SourceDbSerializer,
    SourceFormSerializer,
    SqlConnectionSerializer,
    UserSerializer,
    ValidationRulesSerializer,
)

# Import new serializers
from .canvas_serializers import CanvasCreateSerializer, CanvasEdgeSerializer, CanvasNodeSerializer, CanvasSerializer
from .migration_serializers import (
    MigrationJobCreateSerializer,
    MigrationJobListSerializer,
    MigrationJobLogSerializer,
    MigrationJobSerializer,
    MigrationJobStatusSerializer,
)
from .project_serializers import ProjectCreateSerializer, ProjectDetailSerializer, ProjectSerializer

__all__ = [
    # Legacy
    'SqlConnectionSerializer', 'SourceDbSerializer', 'SourceFormSerializer',
    'ValidationRulesSerializer', 'CountrySerializer', 'SourceConnectionSerializer',
    'DestinationConnectionSerializer', 'FileUploadSerializer', 'CustomerSerializer',
    'UserSerializer',

    # New
    'CanvasSerializer', 'CanvasCreateSerializer', 'CanvasNodeSerializer', 'CanvasEdgeSerializer',
    'MigrationJobSerializer', 'MigrationJobListSerializer', 'MigrationJobCreateSerializer', 'MigrationJobLogSerializer', 'MigrationJobStatusSerializer',
    'ProjectSerializer', 'ProjectCreateSerializer', 'ProjectDetailSerializer'
]
