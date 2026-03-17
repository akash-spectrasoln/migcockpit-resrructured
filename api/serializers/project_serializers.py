"""
Serializers for Project API
"""

from rest_framework import serializers

from api.models.canvas import Canvas
from api.models.project import Project


class ProjectSerializer(serializers.ModelSerializer):
    """Serializer for Project model"""

    id = serializers.IntegerField(source='project_id', read_only=True)  # Alias for frontend compatibility
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    canvas_count = serializers.SerializerMethodField()

    class Meta:
        model = Project
        fields = [
            'id', 'project_id', 'project_name', 'description', 'customer', 'customer_name',
            'created_on', 'modified_on', 'is_active', 'canvas_count'
        ]
        read_only_fields = ['created_on', 'modified_on']

    def get_canvas_count(self, obj):
        """Get number of active canvases in project"""
        return Canvas.objects.filter(project_id=obj.project_id, is_active=True).count()

class ProjectCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating Project"""

    class Meta:
        model = Project
        fields = ['project_name', 'description', 'customer']
        extra_kwargs = {
            'customer': {'required': False, 'allow_null': True},
            'description': {'required': False, 'allow_blank': True},
        }

class ProjectDetailSerializer(serializers.ModelSerializer):
    """Serializer for Project detail view with related canvases"""

    id = serializers.IntegerField(source='project_id', read_only=True)  # Alias for frontend compatibility
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    canvas_count = serializers.SerializerMethodField()
    canvases = serializers.SerializerMethodField()

    class Meta:
        model = Project
        fields = [
            'id', 'project_id', 'project_name', 'description', 'customer', 'customer_name',
            'created_on', 'modified_on', 'is_active', 'canvas_count', 'canvases'
        ]
        read_only_fields = ['created_on', 'modified_on']

    def get_canvas_count(self, obj):
        """Get number of active canvases in project"""
        return Canvas.objects.filter(project_id=obj.project_id, is_active=True).count()

    def get_canvases(self, obj):
        """Get list of canvases in this project"""
        from api.serializers.canvas_serializers import CanvasSerializer
        canvases = Canvas.objects.filter(project_id=obj.project_id, is_active=True)
        return CanvasSerializer(canvases, many=True).data
