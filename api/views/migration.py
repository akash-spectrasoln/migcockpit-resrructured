"""
Migration Job API Views
"""

import asyncio
import logging
import uuid

from django.db.utils import ProgrammingError
import httpx
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from api.authentications import JWTCookieAuthentication
from api.models.canvas import Canvas
from api.models.migration_job import MigrationJob
from api.serializers import (
    MigrationJobCreateSerializer,
    MigrationJobListSerializer,
    MigrationJobLogSerializer,
    MigrationJobSerializer,
    MigrationJobStatusSerializer,
)

logger = logging.getLogger(__name__)

# Migration service URL
MIGRATION_SERVICE_URL = "http://localhost:8003"

class MigrationJobViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Migration Job operations.
    List uses MigrationJobListSerializer (no config/stats) to avoid huge responses and timeouts.
    """
    queryset = MigrationJob.objects.all()
    serializer_class = MigrationJobSerializer
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        if self.action == 'list':
            return MigrationJobListSerializer
        return MigrationJobSerializer

    def get_queryset(self):
        """Filter jobs by customer; optimize list/detail with select_related/prefetch_related to avoid N+1.
        Omit created_by from select_related because the user table may not have an id column in this DB.
        """
        base = MigrationJob.objects.select_related('canvas', 'customer').prefetch_related('logs')
        user = self.request.user
        if user.is_superuser:
            return base
        # user.cust_id is ForeignKey to Customer; use cust_id_id for raw integer pk
        if hasattr(user, 'cust_id_id') and user.cust_id_id is not None:
            return base.filter(customer_id=user.cust_id_id)
        return MigrationJob.objects.none()

    @action(detail=False, methods=['post'])
    def execute(self, request):
        """Execute migration job"""
        serializer = MigrationJobCreateSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        canvas_id = serializer.validated_data['canvas_id']
        config = serializer.validated_data.get('config', {})

        try:
            canvas = Canvas.objects.get(id=canvas_id, is_active=True)
            # Use canvas's customer so we always have a valid Customer instance (integer pk)
            customer = canvas.customer

            # Build source_configs (and optionally destination_configs) so migration service
            # can connect to source DBs. Do NOT run SQL compilation here: Django only triggers;
            # CPU-bound compile runs in Celery before POSTing to FastAPI.
            from api.utils.migration_config_builder import build_migration_config
            config = build_migration_config(canvas, customer, config)

            # Create migration job record.
            # Do not set created_by=user: in this codebase user.pk is the email (string) for
            # in-memory auth, but MigrationJob.created_by_id expects an integer FK.
            job_id = str(uuid.uuid4())
            try:
                migration_job = MigrationJob.objects.create(
                    job_id=job_id,
                    canvas=canvas,
                    customer=customer,
                    status='pending',
                    config=config,
                    created_by=None
                )
            except ProgrammingError as e:
                if 'migration_job' in str(e) and 'does not exist' in str(e).lower():
                    from api.utils.ensure_migration_job_tables import ensure_migration_job_tables
                    ensure_migration_job_tables()
                    migration_job = MigrationJob.objects.create(
                        job_id=job_id,
                        canvas=canvas,
                        customer=customer,
                        status='pending',
                        config=config,
                        created_by=None
                    )
                else:
                    raise

            # Enqueue Celery task to start migration on FastAPI in background. Django returns immediately
            # so the UI stays responsive; FastAPI runs the pipeline asynchronously; user sees status via poll/WebSocket.
            from api.tasks.migration_tasks import execute_migration_task
            execute_migration_task.delay(migration_job.id)

            return Response({
                "job_id": job_id,
                "status": "pending",
                "message": "Migration started; check status or open monitor."
            }, status=status.HTTP_202_ACCEPTED)

        except Canvas.DoesNotExist:
            return Response(
                {"error": "Canvas not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error(f"Error executing migration: {e}")
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=True, methods=['get'])
    def status(self, request, pk=None):
        """Get migration job status from DB (non-blocking). Triggers background refresh when pending/running."""
        job = self.get_object()
        # Trigger background refresh so next poll gets fresh status; never block on FastAPI here.
        if job.status in ('pending', 'running'):
            from api.tasks.migration_tasks import update_migration_status
            update_migration_status.delay(job.id)
        extra = job.status_extra or {}
        status_data = {
            'job_id': job.job_id,
            'status': job.status,
            'progress': job.progress,
            'current_step': job.current_step,
            'error': job.error_message,
            'stats': job.stats,
            'node_progress': extra.get('node_progress'),
            'current_level': extra.get('current_level'),
            'total_levels': extra.get('total_levels'),
            'level_status': extra.get('level_status'),
        }
        serializer = MigrationJobStatusSerializer(status_data)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def logs(self, request, pk=None):
        """Get migration job logs"""
        job = self.get_object()
        logs = job.logs.all()[:100]  # Limit to 100 most recent

        serializer = MigrationJobLogSerializer(logs, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['post'])
    def cancel(self, request, pk=None):
        """Cancel migration job (sync for WSGI compatibility)."""
        job = self.get_object()
        if job.status not in ['pending', 'running']:
            return Response(
                {"error": f"Cannot cancel job in {job.status} status"},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            async def do_cancel():
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(f"{MIGRATION_SERVICE_URL}/{job.job_id}/cancel")
            asyncio.run(do_cancel())
            job.status = 'cancelled'
            job.save()
            return Response({"message": "Job cancelled successfully"})
        except Exception as e:
            logger.error(f"Error cancelling job: {e}")
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
