"""
Canvas Models for storing canvas configurations
"""


from django.conf import settings
from django.db import models
from django.utils import timezone


class Canvas(models.Model):
    """Canvas model for storing data flow canvas configurations"""

    name = models.CharField(max_length=255, verbose_name='Canvas Name')
    description = models.TextField(blank=True, null=True, verbose_name='Description')
    customer = models.ForeignKey('Customer', on_delete=models.CASCADE, related_name='canvases', verbose_name='Customer')
    project_id = models.IntegerField(null=True, blank=True, db_column='project_id', verbose_name='Project ID')
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name='created_canvases', verbose_name='Created By')
    created_on = models.DateTimeField(default=timezone.now, verbose_name='Created On')
    modified_on = models.DateTimeField(auto_now=True, verbose_name='Modified On')
    modified_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name='modified_canvases', verbose_name='Modified By')
    is_active = models.BooleanField(default=True, verbose_name='Active')
    configuration = models.JSONField(default=dict, verbose_name='Canvas Configuration')

    class Meta:
        db_table = 'canvas'
        verbose_name = 'Canvas'
        verbose_name_plural = 'Canvases'
        ordering = ['-created_on']

    def __str__(self):
        return self.name

    def get_nodes(self):
        """Get nodes from configuration"""
        return self.configuration.get('nodes', [])

    def get_edges(self):
        """Get edges from configuration"""
        return self.configuration.get('edges', [])

class CanvasNode(models.Model):
    """Individual node in a canvas - Node Identity Model"""

    canvas = models.ForeignKey(Canvas, on_delete=models.CASCADE, related_name='canvas_nodes', verbose_name='Canvas')
    node_id = models.CharField(max_length=100, unique=True, verbose_name='Node ID (UUID)', help_text='Immutable UUID, must not change')
    business_name = models.CharField(max_length=255, verbose_name='Business Name (Editable)', help_text='Human-friendly name shown on canvas')
    technical_name = models.CharField(max_length=255, verbose_name='Technical Name (Read-only)', help_text='System-generated internal name, format: {type}_{shortId}')
    node_name = models.CharField(max_length=255, verbose_name='Node Name (Legacy)', null=True, blank=True, help_text='Legacy field, use business_name instead')
    node_type = models.CharField(max_length=50, choices=[
        ('SOURCE', 'Source'),
        ('FILTER', 'Filter'),
        ('PROJECTION', 'Projection'),
        ('JOIN', 'Join'),
        ('CALCULATED_COLUMN', 'Calculated Column'),
        ('DESTINATION', 'Destination'),
        ('GROUP', 'Group'),
        ('SORT', 'Sort'),
        ('UNION', 'Union'),
        ('AGGREGATION', 'Aggregation'),
        ('TRANSFORM', 'Transform'),  # Legacy support
    ], verbose_name='Node Type')
    config_json = models.JSONField(default=dict, verbose_name='Node Configuration')
    input_nodes = models.JSONField(default=list, verbose_name='Input Node IDs')
    output_metadata = models.JSONField(default=dict, null=True, blank=True, verbose_name='Output Metadata (Schema)')
    position_x = models.FloatField(verbose_name='Position X')
    position_y = models.FloatField(verbose_name='Position Y')
    created_at = models.DateTimeField(default=timezone.now, verbose_name='Created At')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='Updated At')

    class Meta:
        db_table = 'pipeline_nodes'  # Match specification
        verbose_name = 'Pipeline Node'
        verbose_name_plural = 'Pipeline Nodes'
        unique_together = ['canvas', 'node_id']
        indexes = [
            models.Index(fields=['canvas', 'node_id']),
            models.Index(fields=['node_id']),
        ]

    def __str__(self):
        return f"{self.canvas.name} - {self.node_type} - {self.business_name}"

class CanvasEdge(models.Model):
    """Connection edge between nodes in a canvas"""

    canvas = models.ForeignKey(Canvas, on_delete=models.CASCADE, related_name='canvas_edges', verbose_name='Canvas')
    source_node = models.ForeignKey(CanvasNode, on_delete=models.CASCADE, related_name='outgoing_edges', verbose_name='Source Node')
    target_node = models.ForeignKey(CanvasNode, on_delete=models.CASCADE, related_name='incoming_edges', verbose_name='Target Node')
    edge_id = models.CharField(max_length=100, verbose_name='Edge ID')
    created_on = models.DateTimeField(default=timezone.now, verbose_name='Created On')

    class Meta:
        db_table = 'canvas_edge'
        verbose_name = 'Canvas Edge'
        verbose_name_plural = 'Canvas Edges'
        unique_together = ['canvas', 'edge_id']

    def __str__(self):
        return f"{self.source_node.business_name} -> {self.target_node.business_name}"
