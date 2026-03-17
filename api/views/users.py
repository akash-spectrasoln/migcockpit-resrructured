"""
User management views.
"""
from django.conf import settings
from django.contrib.auth.tokens import default_token_generator
from django.core.mail import send_mail
from django.db import connection
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentications import JWTCookieAuthentication
from api.models import User
from api.serializers import UserSerializer


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
            try:
                serializer.validated_data['email'] = serializer.validated_data['email'].lower()
                serializer.validated_data['cust_id'] = request.user.cust_id
                serializer.validated_data['created_by'] = request.user.email
                serializer.save()
            except Exception as e:
                return Response(
                    {"error": f"Error creating user: {e!s}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class UserListView(APIView):
    """API view for listing users."""
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = request.user
        customer = user.cust_id
        if not customer:
            return Response(
                {"error": "user is not associated with any customer"},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            company_users = User.objects.filter(cust_id=customer).exclude(email=user.email)
            serializer = UserSerializer(company_users, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error retrieving users: {e!s}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class UserUpdateView(APIView):
    """API view for updating users."""
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, user_id):
        """Get user details."""
        try:
            user = User.objects.get(id=user_id)
            serializer = UserSerializer(user)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except User.DoesNotExist:
            return Response({
                'error': 'User not found'
            }, status=status.HTTP_404_NOT_FOUND)

    def put(self, request, user_id):
        """Update user details."""
        try:
            user = User.objects.get(id=user_id)
            serializer = UserSerializer(user, data=request.data, partial=True)

            if serializer.is_valid():
                serializer.validated_data['modified_by'] = request.user.email
                serializer.save()
                return Response({
                    'message': 'User updated successfully',
                    'data': serializer.data
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    'error': 'Validation failed',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)

        except User.DoesNotExist:
            return Response({
                'error': 'User not found'
            }, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({
                'error': f'Error updating user: {e!s}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserDeleteView(APIView):
    """API view for deleting users."""
    authentication_classes = [JWTCookieAuthentication]
    permission_classes = [IsAuthenticated]

    def delete(self, request, user_id):
        """Delete a user."""
        try:
            user = User.objects.get(id=user_id)
            user.delete()
            return Response({
                'message': 'User deleted successfully'
            }, status=status.HTTP_200_OK)
        except User.DoesNotExist:
            return Response({
                'error': 'User not found'
            }, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({
                'error': f'Error deleting user: {e!s}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class UserPasswordResetView(APIView):
    """
    API endpoint for resetting user password.
    """
    def post(self, request):
        """Request password reset."""
        email = request.data.get('email')
        if not email:
            return Response({
                'error': 'Email is required'
            }, status=status.HTTP_400_BAD_REQUEST)

        email = email.lower()
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            return Response({
                'error': 'User with this email address does not exist'
            }, status=status.HTTP_404_NOT_FOUND)

        uid = urlsafe_base64_encode(force_bytes(user.email))
        token = default_token_generator.make_token(user)

        FRONTEND_URL = 'http://localhost:8000/api'

        reset_url = f"{FRONTEND_URL}/reset-password-confirm/{uid}/{token}"

        # Send password reset email
        try:
            send_mail(
                subject='Password Reset Request',
                message=f'Hello,\n\nYou requested to reset your password. Click the link below to reset your password:\n\n{reset_url}\n\nIf you did not request this, please ignore this email.',
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[email],
                fail_silently=False,
            )
        except Exception as e:
            return Response(
                {"error": "Failed to send email. Please try again later." + str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            {"message": "Password reset link has been sent to your email."},
            status=status.HTTP_200_OK
        )

class UserPasswordResetConfirmView(APIView):
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
        # Use raw SQL to update password since user table doesn't have integer id column
        user.set_password(new_password)
        try:
            with connection.cursor() as cursor:
                # Get the encrypted password from the user object
                cursor.execute("""
                    UPDATE "GENERAL"."user"
                    SET password = %s, created_on = %s
                    WHERE email = %s
                """, [user.password, user.created_on, user.email])
        except Exception as save_error:
            print(f"Error updating password: {save_error}")
            return Response(
                {"error": "Failed to update password."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            {"message": "Password has been reset successfully."},
            status=status.HTTP_200_OK
        )
