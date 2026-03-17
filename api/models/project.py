"""
Project Models for organizing sources, destinations, and canvases
"""

from django.conf import settings
from django.db import models
from django.utils import timezone


class Project(models.Model):
    """Project model for organizing migration work"""

    project_id = models.AutoField(primary_key=True, verbose_name='Project ID')
    project_name = models.CharField(max_length=255, verbose_name='Project Name')
    description = models.TextField(blank=True, null=True, verbose_name='Description')
    customer = models.ForeignKey('Customer', on_delete=models.CASCADE, related_name='projects', verbose_name='Customer')
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name='created_projects', verbose_name='Created By')
    created_on = models.DateTimeField(default=timezone.now, verbose_name='Created On')
    modified_on = models.DateTimeField(auto_now=True, verbose_name='Modified On')
    modified_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name='modified_projects', verbose_name='Modified By')
    is_active = models.BooleanField(default=True, verbose_name='Active')

    class Meta:
        db_table = 'project'  # Match migration 0037
        verbose_name = 'Project'
        verbose_name_plural = 'Projects'
        ordering = ['-created_on']
        unique_together = [('customer', 'project_name')]  # Match migration 0037

    def __str__(self):
        return self.project_name

    def get_canvas_count(self):
        """Get number of canvases in this project"""
        return self.canvases.filter(is_active=True).count()

    def get_source_count(self):
        """Get number of sources in this project"""
        # Sources are stored in customer database, we'll count via API
        return 0  # Placeholder - will be calculated via API

    def get_destination_count(self):
        """Get number of destinations in this project"""
        # Destinations are stored in customer database, we'll count via API
        return 0  # Placeholder - will be calculated via API
