from functools import wraps

from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views import View
from rest_framework.exceptions import AuthenticationFailed

from api.authentications import JWTCookieAuthentication


def admin_auth_required(view_func):
    """
    Decorator to check authentication for admin frontend views.
    Uses the same authentication logic as Sessioncheckview.
    Redirects to login page if user is not authenticated.
    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        try:
            # Create authentication instance
            auth = JWTCookieAuthentication()

            # Authenticate the user using JWT cookie authentication
            auth_result = auth.authenticate(request)

            if auth_result is None:
                # No valid authentication found, redirect to login
                return redirect('admin-login')

            user, token = auth_result

            # Check if user is authenticated and is superuser (admin)
            if not user.is_authenticated or not user.is_superuser:
                return redirect('admin-login')

            # User is authenticated, add user to request and proceed
            request.user = user
            return view_func(request, *args, **kwargs)

        except (AuthenticationFailed, Exception):
            # Authentication failed or any other error, redirect to login
            return redirect('admin-login')

    return wrapper

class AdminLoginView(View):
    def get(self, request):
        return render(request, 'admin_login.html')

@method_decorator(admin_auth_required,name='dispatch')
class AdminDashboardView(View):
    def get(self, request):
        return render(request, 'admin_dashboard.html')

@method_decorator(admin_auth_required,name='dispatch')
class CreateCustomerFormView(View):
    def get(self, request):
        return render(request, 'create_customer.html')

@method_decorator(admin_auth_required,name='dispatch')
class CustomerListpageView(View):
    def get(self, request):
        return render(request, 'customerlist.html')

@method_decorator(admin_auth_required,name='dispatch')
class UserListpageView(View):
    def get(self, request):
        return render(request, 'userslist.html')

@method_decorator(admin_auth_required,name='dispatch')
class CreateUserFormView(View):
    def get(self, request):
        return render(request, 'create_user.html')

class PasswordResetRequestView(View):
    def get(self, request):
        return render(request, 'password_reset_request.html')

class PasswordResetConfirmFormView(View):
    def get(self, request, uid=None, token=None):
        """
        Render password reset confirm page.
        uid and token are extracted from URL but handled by JavaScript.
        """
        return render(request, 'password_reset_confirm.html')

@method_decorator(admin_auth_required,name='dispatch')
class EditCustomerFormView(View):
    def get(self, request):
        return render(request, 'edit_customer.html')
