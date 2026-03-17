"""
Project API Views
Merged from: project_views.py, projects.py
"""

import logging

from django.conf import settings
import psycopg2
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.models.canvas import Canvas
from api.models.project import Project
from api.serializers.project_serializers import ProjectCreateSerializer, ProjectDetailSerializer, ProjectSerializer

logger = logging.getLogger(__name__)

# Import ensure_user_has_customer - using late import to avoid circular dependency
def get_ensure_user_has_customer():
    from api.utils.helpers import ensure_user_has_customer
    return ensure_user_has_customer

class ProjectViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Project CRUD operations
    """
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        """Use different serializer for create and retrieve"""
        if self.action == 'create':
            return ProjectCreateSerializer
        elif self.action == 'retrieve':
            return ProjectDetailSerializer
        return ProjectSerializer

    def get_queryset(self):
        """Filter projects by customer"""
        try:
            try:
                Project.objects.exists()
            except Exception as table_error:
                logger.warning(f"Project table may not exist: {table_error}")
                return Project.objects.none()

            user = self.request.user
            if user.is_superuser:
                return Project.objects.filter(is_active=True)

            ensure_user_has_customer = get_ensure_user_has_customer()
            customer = ensure_user_has_customer(user)
            if customer:
                return Project.objects.filter(customer=customer, is_active=True)
            return Project.objects.none()
        except Exception as e:
            logger.error(f"Error in get_queryset: {e}")
            return Project.objects.none()

    def perform_create(self, serializer):
        """Set customer from user"""
        user = self.request.user
        ensure_user_has_customer = get_ensure_user_has_customer()
        customer = ensure_user_has_customer(user)
        if customer:
            serializer.save(customer=customer)
        else:
            serializer.save()

    def perform_destroy(self, instance):
        """Soft delete project by setting is_active=False"""
        instance.is_active = False
        instance.save()

    @action(detail=True, methods=['get'])
    def canvases(self, request, pk=None):
        """Get all canvases for this project"""
        project = self.get_object()
        canvases = Canvas.objects.filter(project_id=project.project_id, is_active=True)
        from api.serializers.canvas_serializers import CanvasSerializer
        serializer = CanvasSerializer(canvases, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def stats(self, request, pk=None):
        """Get project statistics"""
        project = self.get_object()
        canvas_count = Canvas.objects.filter(project_id=project.project_id, is_active=True).count()
        stats = {
            'canvas_count': canvas_count,
            'source_count': 0,
            'destination_count': 0,
        }
        return Response(stats)

class ProjectsListView(APIView):
    """
    Legacy API endpoint for listing projects from customer database.
    """

    def get(self, request):
        user = request.user
        customer = user.cust_id
        if not customer:
            return Response(
                {"error": "user is not associated with any customer"},
                status=status.HTTP_400_BAD_REQUEST
            )

        connection = psycopg2.connect(
            host=settings.DATABASES['default']['HOST'],
            port=settings.DATABASES['default']['PORT'],
            user=settings.DATABASES['default']['USER'],
            password=settings.DATABASES['default']['PASSWORD'],
            database=customer.cust_db
        )
        cursor = connection.cursor()
        cursor.execute('''SELECT "ID","IDENT","DESCR","IS_ACTIVE","REF_CLIENT" FROM "GENERAL"."PROJECT"''')
        projects_raw = cursor.fetchall()
        cursor.close()
        connection.close()

        projects = [
            {'id': p[0], 'ident': p[1], 'descr': p[2], 'is_active': p[3], 'ref_client': p[4]}
            for p in projects_raw
        ]
        return Response(projects, status=status.HTTP_200_OK)
