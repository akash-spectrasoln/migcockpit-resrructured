"""
Frontend template views for the core Django app.
These serve HTML templates for the legacy template-based frontend.
"""
from functools import wraps

from django.shortcuts import redirect, render
from rest_framework.exceptions import AuthenticationFailed

from api.authentications import JWTCookieAuthentication


def company_admin_auth_required(view_func):
    """
    Decorator to check authentication for frontend views.
    Redirects to login page if user is not authenticated.
    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        try:
            auth = JWTCookieAuthentication()
            auth_result = auth.authenticate(request)
            if auth_result is None:
                return redirect('login_page')
            user, token = auth_result
            if not user.is_authenticated or user.is_superuser:
                return redirect('login_page')
            request.user = user
            return view_func(request, *args, **kwargs)
        except (AuthenticationFailed, Exception):
            return redirect('login_page')
    return wrapper

def login_page(request):
    return render(request, 'login.html')

def sql_connection_form(request):
    return render(request, 'sql_connection_form.html')

@company_admin_auth_required
def source_connection_form(request):
    return render(request, 'add_source.html')

@company_admin_auth_required
def customer_sources(request):
    return render(request, 'customer_sources.html')

@company_admin_auth_required
def edit_source(request, source_id):
    return render(request, 'edit_source.html', {'source_id': source_id})

@company_admin_auth_required
def destination_connection_form(request):
    return render(request, 'add_destination.html')

@company_admin_auth_required
def customer_destinations(request):
    return render(request, 'customer_destinations.html')

@company_admin_auth_required
def edit_destination(request, destination_id):
    return render(request, 'edit_destination.html', {'destination_id': destination_id})

def file_upload(request):
    return render(request, 'file_upload.html')

def table_management(request):
    return render(request, 'table_management.html')

def user_tables(request, project_id):
    return render(request, 'user_tables.html', {'project_id': project_id})

def table_data_display(request, project_id):
    return render(request, 'table_data_display.html', {'project_id': project_id})

def create_table(request):
    return render(request, 'create_table.html')

def import_data(request):
    return render(request, 'import_data.html')

def table_navigation(request, project_id):
    return render(request, 'table_navigation.html', {'project_id': project_id})

@company_admin_auth_required
def customer_user_dashboard(request):
    return render(request, 'dashboard.html')

@company_admin_auth_required
def users_list(request):
    return render(request, 'users_list.html')

@company_admin_auth_required
def create_user(request):
    return render(request, 'create_user.html')

@company_admin_auth_required
def edit_user(request, user_id):
    return render(request, 'edit_user.html', {'user_id': user_id})

@company_admin_auth_required
def user_delete(request, user_id):
    return render(request, 'users_list.html')

@company_admin_auth_required
def projects_list(request):
    return render(request, 'projects_list.html')

def password_reset_request(request):
    return render(request, 'password_reset_request.html')

def password_reset_confirm(request, uidb64, token):
    return render(request, 'password_reset_confirm.html', {
        'uidb64': uidb64, 'token': token
    })

@company_admin_auth_required
def validation_rules_page(request):
    return render(request, 'validation_rules.html')
