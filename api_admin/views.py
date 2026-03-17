from django.conf import settings
from django.contrib.auth import authenticate
from django.contrib.auth.tokens import default_token_generator
from django.core.mail import send_mail
from django.db import transaction
from django.utils import timezone
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken

from api.authentications import JWTCookieAuthentication
from api.models import Country, Customer, User
from api_admin.serializers import CountrySerializer, CustomerSerializer, UserSerializer

# Create your views here.

class CreateCustomerView(APIView):
    """API view for creating customers and associated users."""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """Create a new customer and associated user in a single transaction."""
        database_created = False
        database_name = None

        try:
            # Extract customer and user data from request
            customer_data = request.data.get('customer', {})
            user_data = request.data.get('user', {})

            if not customer_data or not user_data:
                return Response(
                    {"error": "Both customer and user data are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Use transaction.atomic to ensure customer and user are created together
            # Note: Database creation happens outside this transaction due to PostgreSQL requirements
            with transaction.atomic():
                # Create customer
                customer_serializer = CustomerSerializer(data=customer_data)
                customer_serializer.is_valid(raise_exception=True)
                customer_serializer.validated_data['created_by'] = request.user.email
                customer = customer_serializer.save()
                database_name = customer.cust_db  # Store for potential cleanup

                # Create user with the newly created customer
                user_data['cust_id'] = customer.id
                user_serializer = UserSerializer(data=user_data)
                user_serializer.is_valid(raise_exception=True)

                user_serializer.validated_data['created_by'] = request.user.email
                user_serializer.validated_data['is_staf'] = True # Set is_staff to True for the company's admin
                user_serializer.save()

                # Create customer database and schemas
                # Note: This uses autocommit and cannot be rolled back by Django
                # If it fails, we'll catch it and let Django rollback customer/user
                try:
                    customer.create_customer_database()
                    database_created = True
                except Exception as db_error:
                    # Database creation failed, Django will rollback customer/user automatically
                    raise Exception(f"Failed to create customer database: {db_error!s}")

                # If we reach here, everything succeeded
                return Response(
                    {
                        "message": "Customer, user, and database created successfully",
                    },
                    status=status.HTTP_201_CREATED
                )

        except Exception as e:
            # If database was created but transaction failed, clean it up manually
            if database_created and database_name:
                try:
                    self._cleanup_database(database_name)
                except Exception as cleanup_error:
                    print(f"Warning: Failed to cleanup database {database_name}: {cleanup_error!s}")

            return Response(
                {"error": f"Error creating customer and user: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def _cleanup_database(self, database_name):
        """Helper method to cleanup a database if transaction fails."""
        from django.conf import settings
        import psycopg2

        try:
            conn = psycopg2.connect(
                host=settings.DATABASES['default']['HOST'],
                port=settings.DATABASES['default']['PORT'],
                user=settings.DATABASES['default']['USER'],
                password=settings.DATABASES['default']['PASSWORD'],
                database='postgres'
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Terminate any connections to the database before dropping
            cursor.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = %s
                AND pid <> pg_backend_pid();
            """, (database_name,))

            # Drop the database
            cursor.execute(f'DROP DATABASE IF EXISTS "{database_name}";')
            print(f"Cleaned up database: {database_name}")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error cleaning up database {database_name}: {e!s}")
            raise

class CountryListView(APIView):
    """API view for listing countries."""

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get list of all countries."""
        try:
            countries = Country.objects.all()
            serializer = CountrySerializer(countries, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error fetching countries: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class LoginView(APIView):
    """
    API endpoint for user login with JWT authentication.
    """

    def post(self, request):
        """
        Handle user login and return JWT tokens.
        """
        email = request.data.get('email').lower()

        password = request.data.get('password')

        if not email or not password:
            return Response({
                'error': 'Please provide both email and password'
            }, status=status.HTTP_400_BAD_REQUEST)

        # Authenticate user
        user = authenticate(request, email=email, password=password)

        if not user:
            return Response({
                'error': 'Invalid email or password'
            }, status=status.HTTP_401_UNAUTHORIZED)

        if not user.is_superuser:
            return Response({
                'error': 'You are not authorized to access this resource'
            }, status=status.HTTP_401_UNAUTHORIZED)

        if user is not None:
            if user.is_active:
                # Update last_login field
                user.last_login = timezone.now()
                user.save(update_fields=['last_login'])

                # Generate JWT tokens
                refresh = RefreshToken.for_user(user)

                # Create response
                response = Response({
                    'message': 'Login successful',
                    'user': {
                        'email': user.email,
                        'first_name': user.first_name,
                        'last_name': user.last_name,
                        'username': user.username,
                    }
                }, status=status.HTTP_200_OK)

                # Set HTTP-only cookies for tokens
                response.set_cookie(
                    key='access_token',
                    value=str(refresh.access_token),
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
        else:
            return Response({
                'error': 'Invalid email or password'
            }, status=status.HTTP_401_UNAUTHORIZED)

class LogoutView(APIView):
    """
    API endpoint for user logout.
    Clears JWT tokens stored in cookies and Django session.
    """

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

class Sessioncheckview(APIView):
    """
    API endpoint for checking if the user is logged in.
    """

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Check if the user is logged in.
        """
        return Response({
            'message': 'User is logged in'
        }, status=status.HTTP_200_OK)

class CustomerListView(APIView):
    """
    API endpoint for listing customers.
    """

    def get(self, request):
        """
        Get list of all customers."""

        try:
            customers = Customer.objects.all()
            serializer = CustomerSerializer(customers, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error retrieving customers: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class CreateUserView(APIView):
    """
    API endpoint for creating users.
    """

    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Create a new user.
        """
        serializer = UserSerializer(data=request.data)

        if serializer.is_valid():
            print(serializer.validated_data,"]]]]]]]]]]]]]]]]]]]]]]]]]]]]")
            serializer.validated_data['created_by'] = request.user.email
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class UserListView(APIView):
    def get(self,request):
        try:
            users = User.objects.filter(is_superuser=False,is_staff=True)
            serializer = UserSerializer(users, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error retrieving users: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class PasswordResetView(APIView):
    """
    API endpoint for resetting user password.
    """

    def post(self, request):
        """
        Reset user password and send reset email.
        """
        email = request.data.get('email')

        if not email:
            return Response(
                {"error": "Email is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            # Return success even if user not found (security best practice)
            return Response(
                {"message": "If an account exists with this email, you will receive a password reset link."},
                status=status.HTTP_200_OK
            )

        # Encode user email for password reset URL
        uid = urlsafe_base64_encode(force_bytes(user.email))
        token = default_token_generator.make_token(user)

        # Build password reset URL
        FRONTEND_URL = 'http://localhost:8000/api-admin'
        reset_url = f"{FRONTEND_URL}/reset-password/{uid}/{token}"

        # Send password reset email
        try:
            send_mail(
                subject='Password Reset Request',
                message=f'Hello,\n\nYou requested to reset your password. Click the link below to reset your password:\n\n{reset_url}\n\nIf you did not request this, please ignore this email.',
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=["hancelxavier@gmail.com"],
                fail_silently=False,
            )
        except Exception as e:
            return Response(
                {"error": "Failed to send email. Please try again later." + str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            {"message": "If an account exists with this email, you will receive a password reset link."},
            status=status.HTTP_200_OK
        )

class PasswordResetConfirmView(APIView):
    """
    API endpoint for confirming password reset with token.
    """

    def post(self, request):
        """
        Confirm password reset and set new password.
        """
        uidb64 = request.data.get('uid')
        token = request.data.get('token')
        new_password = request.data.get('new_password')

        # Validate required fields
        if not all([uidb64, token, new_password]):
            return Response(
                {"error": "uid, token, and new_password are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # Decode the email from base64
            email = urlsafe_base64_decode(uidb64).decode()
            user = User.objects.get(email=email)
        except (User.DoesNotExist, ValueError, TypeError, UnicodeDecodeError):
            return Response(
                {"error": "Invalid reset link."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Verify token
        if not default_token_generator.check_token(user, token):
            return Response(
                {"error": "Invalid or expired reset link."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Set new password
        user.set_password(new_password)
        user.save()

        return Response(
            {"message": "Password has been reset successfully."},
            status=status.HTTP_200_OK
        )

class CustomerUserUpdateView(APIView):
    """
    API endpoint for updating customer and customer adminuser.
    """
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, id):
        """
        Get customer and associated user data for editing.
        """
        if not id:
            return Response({"error": "cust_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        try:
            customer = Customer.objects.get(id=id)
            serializer = CustomerSerializer(customer)

            users = User.objects.filter(cust_id=customer, is_staff=True).order_by('created_on').first()
            user_serializer = UserSerializer(users)
            return Response({
                "customer": serializer.data,
                "users": user_serializer.data
            }, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error retrieving customer: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def put(self, request,id):
        """
        Update customer and associated user data.
        """
        try:
            user_id = request.data.get('user_id')
            customer_data = request.data.get('customer', {})
            user_data = request.data.get('user', {})

            if not id:
                return Response(
                    {"error": "cust_id is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not user_id:
                return Response(
                    {"error": "user_id is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if not customer_data or not user_data:
                return Response(
                    {"error": "Both customer and user data are required"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            try:
                customer = Customer.objects.get(id=id)
            except Customer.DoesNotExist:
                return Response(
                        {"error": "Customer not found"},
                        status=status.HTTP_404_NOT_FOUND
                    )

            # Get the user directly by ID
            try:
                user = User.objects.get(id=user_id)
            except User.DoesNotExist:
                return Response(
                    {"error": "User not found"},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Use transaction.atomic to ensure both customer and user are updated together
            with transaction.atomic():

                # Update customer
                customer_serializer = CustomerSerializer(customer, data=customer_data, partial=True)
                customer_serializer.is_valid(raise_exception=True)
                customer_serializer.validated_data['modified_by'] = request.user.email
                customer = customer_serializer.save()

                # Update user
                user_serializer = UserSerializer(user, data=user_data, partial=True)
                user_serializer.is_valid(raise_exception=True)
                user_serializer.validated_data['modified_by'] = request.user.email if hasattr(request, 'user') else None
                user = user_serializer.save()

                return Response(
                    {
                        "message": "Customer and user updated successfully",
                        "customer": CustomerSerializer(customer).data,
                        "user": UserSerializer(user).data
                    },
                    status=status.HTTP_200_OK
                )

        except Exception as e:
            return Response(
                {"error": f"Error updating customer and user: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
