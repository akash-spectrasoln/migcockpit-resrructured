from django.db import connection
from rest_framework.exceptions import AuthenticationFailed
from rest_framework_simplejwt.authentication import JWTAuthentication

from api.models import User


class JWTCookieAuthentication(JWTAuthentication):
    """
    Custom JWT authentication class that reads the JWT token from HttpOnly cookies only.
    """
    def authenticate(self, request):
        # Get token from cookie
        access_token = request.COOKIES.get('access_token')

        if not access_token:
            return None

        try:
            # Validate the token
            validated_token = self.get_validated_token(access_token)
            # Get the user from the validated token
            user = self.get_user(validated_token)
            return (user, validated_token)
        except Exception as e:
            raise AuthenticationFailed(f'Invalid or expired token: {e!s}')

    def get_user(self, validated_token):
        """
        Override to fetch user by email instead of id, using the legacy GENERAL.user table.
        We construct an in-memory User object; we do NOT hit the Django ORM table,
        because that schema does not have an integer `id` column.
        """
        try:
            # Get user_id from token (which is actually the email)
            user_id = validated_token.get('user_id')
            if not user_id:
                raise AuthenticationFailed('Token contained no recognizable user identification')

            # Fetch user by email using raw SQL against GENERAL.user
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT email, password, is_active, is_superuser, is_staff,
                           first_name, last_name, created_on, cust_id_id
                    FROM "GENERAL"."user"
                    WHERE email = %s
                """, [user_id])
                row = cursor.fetchone()

                if not row:
                    raise AuthenticationFailed('User not found')

                db_email, db_password, is_active, is_superuser, is_staff, first_name, last_name, created_on, cust_id = row

                # Create a temporary User object to return
                user = User()
                user.email = db_email
                user.password = db_password
                user.created_on = created_on
                user.is_active = is_active
                user.is_superuser = is_superuser
                user.is_staff = is_staff
                user.first_name = first_name
                user.last_name = last_name
                # Use email as pk for in-memory object; DB schema doesn't have integer id
                user.pk = db_email
                user._state.adding = False
                user._state.db = 'default'

                # Set cust_id if available
                if cust_id:
                    user.cust_id_id = cust_id

                return user
        except Exception as e:
            raise AuthenticationFailed(f'User not found: {e!s}')
