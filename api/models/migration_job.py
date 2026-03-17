"""
Migration Job Models for tracking migration executions
"""


from django.conf import settings
from django.db import models
from django.utils import timezone


class MigrationJob(models.Model):
    """Migration job model for tracking data migration executions"""

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]

    job_id = models.CharField(max_length=100, unique=True, verbose_name='Job ID')
    canvas = models.ForeignKey('Canvas', on_delete=models.CASCADE, related_name='migration_jobs', verbose_name='Canvas')
    customer = models.ForeignKey('Customer', on_delete=models.CASCADE, related_name='migration_jobs', verbose_name='Customer')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', verbose_name='Status')
    progress = models.FloatField(default=0.0, verbose_name='Progress (%)')
    current_step = models.CharField(max_length=255, blank=True, null=True, verbose_name='Current Step')

    # Configuration
    config = models.JSONField(default=dict, verbose_name='Job Configuration')

    # Results
    stats = models.JSONField(default=dict, blank=True, null=True, verbose_name='Statistics')
    # Pass-through from migration service: node_progress, current_level, total_levels, level_status (for non-blocking status)
    status_extra = models.JSONField(default=dict, blank=True, null=True, verbose_name='Status extra (node_progress, levels)')
    error_message = models.TextField(blank=True, null=True, verbose_name='Error Message')

    # Timestamps
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name='created_migration_jobs', verbose_name='Created By')
    created_on = models.DateTimeField(default=timezone.now, verbose_name='Created On')
    started_on = models.DateTimeField(blank=True, null=True, verbose_name='Started On')
    completed_on = models.DateTimeField(blank=True, null=True, verbose_name='Completed On')

    class Meta:
        db_table = 'migration_job'
        verbose_name = 'Migration Job'
        verbose_name_plural = 'Migration Jobs'
        ordering = ['-created_on']

    def __str__(self):
        return f"{self.job_id} - {self.canvas.name} - {self.status}"

class MigrationJobLog(models.Model):
    """Log entries for migration jobs"""

    LOG_LEVEL_CHOICES = [
        ('DEBUG', 'Debug'),
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    ]

    job = models.ForeignKey(MigrationJob, on_delete=models.CASCADE, related_name='logs', verbose_name='Job')
    level = models.CharField(max_length=10, choices=LOG_LEVEL_CHOICES, default='INFO', verbose_name='Log Level')
    message = models.TextField(verbose_name='Message')
    timestamp = models.DateTimeField(default=timezone.now, verbose_name='Timestamp')
    metadata = models.JSONField(default=dict, blank=True, null=True, verbose_name='Metadata')

    class Meta:
        db_table = 'migration_job_log'
        verbose_name = 'Migration Job Log'
        verbose_name_plural = 'Migration Job Logs'
        ordering = ['-timestamp']

    def __str__(self):
        return f"{self.job.job_id} - {self.level} - {self.timestamp}"
