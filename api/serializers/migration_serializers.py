"""
Serializers for Migration Job API
"""

from rest_framework import serializers

from api.models.canvas import Canvas
from api.models.migration_job import MigrationJob, MigrationJobLog


class MigrationJobSerializer(serializers.ModelSerializer):
    """Serializer for MigrationJob model (full; used for retrieve/detail)"""

    canvas_name = serializers.CharField(source='canvas.name', read_only=True)
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    created_by_name = serializers.CharField(source='created_by.email', read_only=True)
    log_count = serializers.SerializerMethodField()

    class Meta:
        model = MigrationJob
        fields = [
            'id', 'job_id', 'canvas', 'canvas_name', 'customer', 'customer_name',
            'status', 'progress', 'current_step', 'config', 'stats',
            'error_message', 'created_by', 'created_by_name',
            'created_on', 'started_on', 'completed_on', 'log_count'
        ]
        read_only_fields = [
            'created_on', 'started_on', 'completed_on', 'job_id'
        ]

    def get_log_count(self, obj):
        """Get number of log entries"""
        return obj.logs.count()

class MigrationJobListSerializer(serializers.ModelSerializer):
    """Light serializer for list view: omit large config/stats to avoid huge responses and timeouts."""

    canvas_name = serializers.CharField(source='canvas.name', read_only=True)
    customer_name = serializers.CharField(source='customer.name', read_only=True)
    created_by_name = serializers.CharField(source='created_by.email', read_only=True)
    log_count = serializers.SerializerMethodField()

    class Meta:
        model = MigrationJob
        fields = [
            'id', 'job_id', 'canvas', 'canvas_name', 'customer', 'customer_name',
            'status', 'progress', 'current_step', 'error_message', 'created_by_name',
            'created_on', 'started_on', 'completed_on', 'log_count'
        ]
        read_only_fields = ['created_on', 'started_on', 'completed_on', 'job_id']

    def get_log_count(self, obj):
        return obj.logs.count()

class MigrationJobCreateSerializer(serializers.Serializer):
    """Serializer for creating migration job"""

    canvas_id = serializers.IntegerField()
    config = serializers.JSONField(required=False, default=dict)

    def validate_canvas_id(self, value):
        """Validate canvas exists"""
        try:
            Canvas.objects.get(id=value, is_active=True)
        except Canvas.DoesNotExist:
            raise serializers.ValidationError("Canvas not found or inactive")
        return value

class MigrationJobLogSerializer(serializers.ModelSerializer):
    """Serializer for MigrationJobLog model"""

    class Meta:
        model = MigrationJobLog
        fields = [
            'id', 'job', 'level', 'message', 'timestamp', 'metadata'
        ]
        read_only_fields = ['timestamp']

class MigrationJobStatusSerializer(serializers.Serializer):
    """Serializer for migration job status response"""

    job_id = serializers.CharField()
    status = serializers.CharField()
    progress = serializers.FloatField()
    current_step = serializers.CharField(allow_null=True)
    error = serializers.CharField(allow_null=True)
    stats = serializers.JSONField(allow_null=True)
    node_progress = serializers.ListField(
        child=serializers.DictField(),
        allow_null=True,
        required=False,
        help_text='Per-node progress from migration service for UI tick marks',
    )
    current_level = serializers.IntegerField(allow_null=True, required=False, help_text='Current execution level (1-based)')
    total_levels = serializers.IntegerField(allow_null=True, required=False, help_text='Total execution levels')
    level_status = serializers.CharField(allow_null=True, required=False, help_text='Level status: running or complete')
