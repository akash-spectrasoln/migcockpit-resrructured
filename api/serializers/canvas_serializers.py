"""
Serializers for Canvas API
"""

from rest_framework import serializers

from api.models import Project
from api.models.canvas import Canvas, CanvasEdge, CanvasNode


class CanvasSerializer(serializers.ModelSerializer):
    """Serializer for Canvas model"""

    customer_name = serializers.CharField(source='customer.name', read_only=True)
    project_name = serializers.SerializerMethodField()
    created_by_name = serializers.CharField(source='created_by.email', read_only=True)
    node_count = serializers.SerializerMethodField()
    edge_count = serializers.SerializerMethodField()

    class Meta:
        model = Canvas
        fields = [
            'id', 'name', 'description', 'customer', 'customer_name',
            'project_id', 'project_name', 'created_by', 'created_by_name',
            'created_on', 'modified_on', 'modified_by', 'is_active',
            'configuration', 'node_count', 'edge_count'
        ]
        read_only_fields = ['created_on', 'modified_on']

    def get_node_count(self, obj):
        """Get number of nodes in canvas"""
        return len(obj.get_nodes())

    def get_edge_count(self, obj):
        """Get number of edges in canvas"""
        return len(obj.get_edges())

    def get_project_name(self, obj):
        """Get project name from project_id"""
        if obj.project_id:
            try:
                # Check if Project table exists by trying to query it
                # Try by id first (Django default), then by project_id if it exists
                project = Project.objects.filter(pk=obj.project_id).first()
                if not project:
                    # Fallback: try project_id if the table uses that as PK
                    project = Project.objects.filter(project_id=obj.project_id).first()
                if project:
                    return project.project_name
            except Exception as e:
                # If Project table doesn't exist or query fails, just return None
                # Log the error but don't break serialization
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"Could not fetch project name for project_id={obj.project_id}: {e}")
                pass
        return None

class CanvasCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating Canvas"""

    class Meta:
        model = Canvas
        fields = ['name', 'description', 'customer', 'project_id', 'configuration']
        # Customer will be injected from the authenticated user in the ViewSet,
        # so it should not be required from the client when creating a canvas.
        extra_kwargs = {
            'name': {'required': True},  # Name is required
            'customer': {'required': False, 'allow_null': True},
            'project_id': {'required': False, 'allow_null': True},  # Project is optional for backward compatibility
            'description': {'required': False, 'allow_blank': True},
            'configuration': {'required': False},
        }

    def create(self, validated_data):
        """
        Create canvas.
        NOTE: We intentionally do NOT set `created_by` here because the legacy
        user table does not have an integer primary key compatible with the
        `created_by` ForeignKey. The field is nullable on the model.
        """
        return super().create(validated_data)

class CanvasNodeSerializer(serializers.ModelSerializer):
    """Serializer for CanvasNode model"""

    class Meta:
        model = CanvasNode
        fields = [
            'id', 'canvas', 'node_id', 'business_name', 'technical_name',
            'node_name', 'node_type', 'position_x', 'position_y',
            'config_json', 'input_nodes', 'output_metadata', 'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at', 'technical_name']  # technical_name is immutable

class CanvasEdgeSerializer(serializers.ModelSerializer):
    """Serializer for CanvasEdge model"""

    source_node_label = serializers.CharField(source='source_node.business_name', read_only=True)
    target_node_label = serializers.CharField(source='target_node.business_name', read_only=True)

    class Meta:
        model = CanvasEdge
        fields = [
            'id', 'canvas', 'source_node', 'target_node', 'edge_id',
            'source_node_label', 'target_node_label', 'created_on'
        ]
        read_only_fields = ['created_on']
