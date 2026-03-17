"""
Authentication views for user login, logout, and token refresh.
"""
from django.db import connection
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.serializers import TokenRefreshSerializer


class LoginView(APIView):
    """
    API endpoint for user login with JWT authentication.
    """
    authentication_classes = []  # No authentication required for login
    permission_classes = []  # No permissions required for login

    def post(self, request):
        """
        Handle user login and return JWT tokens.
        """
        try:
            email = request.data.get('email')
            password = request.data.get('password')

            if not email or not password:
                return Response({
                    'error': 'Please provide both email and password'
                }, status=status.HTTP_400_BAD_REQUEST)

            # Normalize email to lowercase and trim whitespace
            email = email.lower().strip() if email else ''
            print(f"Email received (normalized): '{email}'")

            # Validate email format
            if not email or '@' not in email:
                return Response({
                    'error': 'Invalid email format'
                }, status=status.HTTP_400_BAD_REQUEST)

            # Custom authentication using raw SQL to avoid id column issue
            # Fetch user directly from database
            user = None
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT email, password, is_active, is_superuser, is_staff,
                               first_name, last_name, created_on, cust_id_id
                        FROM "GENERAL"."user"
                        WHERE email = %s
                    """, [email])
                    row = cursor.fetchone()

                    if not row:
                        print(f"User not found for email: {email}")
                        return Response({
                            'error': f'No account found with email: {email}. Please check your email address.'
                        }, status=status.HTTP_401_UNAUTHORIZED)

                    db_email, db_password, is_active, is_superuser, is_staff, first_name, last_name, created_on, cust_id = row

                    # Check password using User model's check_password method
                    # Create a temporary user object to use check_password
                    from api.models import User
                    # Use a temporary user ONLY for password verification
                    temp_user = User()
                    temp_user.email = db_email
                    temp_user.password = db_password
                    temp_user.created_on = created_on
                    temp_user.is_active = is_active
                    temp_user.is_superuser = is_superuser
                    temp_user.is_staff = is_staff
                    temp_user.first_name = first_name
                    temp_user.last_name = last_name

                    # Verify password
                    print(f"Checking password for user: {db_email}")
                    print(f"Password from DB length: {len(db_password) if db_password else 0}")
                    print(f"Password from DB preview: {db_password[:50] if db_password else 'None'}")
                    print(f"Created_on: {created_on}")

                    password_check_result = temp_user.check_password(password)
                    print(f"Password check result: {password_check_result}")

                    if not password_check_result:
                        print(f"Password check failed for user: {db_email}")
                        return Response({
                            'error': 'Invalid email or password'
                        }, status=status.HTTP_401_UNAUTHORIZED)

                    print(f"Password check passed for user: {db_email}")

                    # Use the SAME pattern as before: an in-memory user object,
                    # with email as the identifier for JWT (no ORM hit on legacy table).
                    user = temp_user
                    user.pk = db_email
                    user._state.adding = False
                    user._state.db = 'default'

            except Exception as auth_error:
                import traceback
                error_trace = traceback.format_exc()
                print(f"Authentication error: {auth_error}")
                print(f"Traceback: {error_trace}")
                return Response({
                    'error': f'Authentication failed: {auth_error!s}'
                }, status=status.HTTP_401_UNAUTHORIZED)

            # Check if user was successfully authenticated
            if user is None:
                return Response({
                    'error': 'Authentication failed: User not found'
                }, status=status.HTTP_401_UNAUTHORIZED)

            # User is authenticated, check if active
            if user.is_active:
                # Update last_login field using raw SQL (skip if fails due to schema issues)
                # We can't use user.save() because user.pk is email (string), not integer id
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            UPDATE "GENERAL"."user"
                            SET last_login = %s
                            WHERE email = %s
                        """, [timezone.now(), user.email])
                except Exception as save_error:
                    print(f"Warning: Could not update last_login: {save_error}")
                    # Continue anyway - this is not critical

                # Generate JWT tokens manually to avoid OutstandingToken issues
                # Since user table doesn't have id column, we create tokens manually
                from rest_framework_simplejwt.tokens import RefreshToken

                # Create refresh token manually
                refresh = RefreshToken()
                # Set token claims based on SIMPLE_JWT settings
                refresh['user_id'] = user.email  # USER_ID_FIELD is 'email'
                refresh['email'] = user.email
                refresh['is_superuser'] = user.is_superuser
                refresh['is_staf'] = user.is_staff

                # Access token is automatically generated from refresh token
                access_token = refresh.access_token

                getattr(user, 'cust_id', None)

                # Create response
                response = Response({
                    'message': 'Login successful',
                    'user': {
                        'email': user.email,
                        'first_name': user.first_name,
                        'last_name': user.last_name,
                    }
                }, status=status.HTTP_200_OK)

                # Set HTTP-only cookies for tokens
                response.set_cookie(
                    key='access_token',
                    value=str(access_token),
                    httponly=True,
                    secure=False,  # Set to True in production with HTTPS
                    samesite='Lax',
                    max_age=3600  # 1 hour (matches ACCESS_TOKEN_LIFETIME)
                )

                response.set_cookie(
                    key='refresh_token',
                    value=str(refresh),
                    httponly=True,
                    secure=False,  # Set to True in production with HTTPS
                    samesite='Lax',
                    max_age=86400  # 1 day (matches REFRESH_TOKEN_LIFETIME)
                )

                return response
            else:
                return Response({
                    'error': 'User account is disabled'
                }, status=status.HTTP_401_UNAUTHORIZED)
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"Login error: {e!s}")
            print(f"Traceback: {error_trace}")
            return Response({
                'error': f'Internal server error: {e!s}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class LogoutView(APIView):
    """
    API endpoint for user logout.
    Clears JWT tokens stored in cookies and Django session.
    """
    authentication_classes = []  # No authentication required for logout
    permission_classes = []  # No permissions required for logout

    def get(self, request):
        """
        Handle user logout by clearing JWT token cookies and Django session.
        """
        try:

            # Flush the Django session if it exists
            if hasattr(request, 'session'):
                request.session.flush()
                print("Django session flushed")

            # Create response
            response = Response({
                'message': 'Logout successful'
            }, status=status.HTTP_200_OK)

            # Delete the access_token cookie
            # IMPORTANT: All parameters must match those used when setting the cookie
            response.delete_cookie(
                key='access_token',
                path='/',
                samesite='Lax'
            )

            # Delete the refresh_token cookie
            # IMPORTANT: All parameters must match those used when setting the cookie
            response.delete_cookie(
                key='refresh_token',
                path='/',
                samesite='Lax'
            )

            # Also delete the sessionid cookie (Django's default session cookie)
            response.delete_cookie(
                key='sessionid',
                path='/',
                samesite='Lax'
            )

            # Delete CSRF cookie
            response.delete_cookie(
                key='csrftoken',
                path='/',
                samesite='Lax'
            )

            return response

        except Exception as e:
            return Response({
                'error': f'Logout failed: {e!s}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class RefreshTokenView(APIView):
    """
    API endpoint to refresh access token using the refresh token stored in an HttpOnly cookie.
    """
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        try:
            refresh_token = request.COOKIES.get('refresh_token')
            if not refresh_token:
                return Response(
                    {'error': 'Refresh token missing', 'redirect_login': True},
                    status=status.HTTP_401_UNAUTHORIZED,
                    headers={'X-Auth-Required': 'true'},
                )

            serializer = TokenRefreshSerializer(data={'refresh': refresh_token})
            serializer.is_valid(raise_exception=True)
            data = serializer.validated_data

            response = Response({'message': 'Token refreshed successfully'}, status=status.HTTP_200_OK)

            # Set new access token
            response.set_cookie(
                key='access_token',
                value=data.get('access'),
                httponly=True,
                secure=False,  # Set to True in production with HTTPS
                samesite='Lax',
                max_age=3600
            )

            # If rotation is enabled, a new refresh may be returned; update cookie
            new_refresh = data.get('refresh')
            if new_refresh:
                response.set_cookie(
                    key='refresh_token',
                    value=new_refresh,
                    httponly=True,
                    secure=False,
                    samesite='Lax',
                    max_age=86400
                )

            return response
        except Exception as e:
            return Response(
                {'error': f'Invalid or expired refresh token: {e!s}', 'redirect_login': True},
                status=status.HTTP_401_UNAUTHORIZED,
                headers={'X-Auth-Required': 'true'},
            )
