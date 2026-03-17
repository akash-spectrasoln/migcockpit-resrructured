"""
Utility views for common operations.
"""
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from api.models import ValidationRules
from api.serializers import ValidationRulesSerializer


class ValidationRulesView(APIView):
    """API view for managing validation rules."""
    # authentication_classes = [JWTCookieAuthentication]
    # permission_classes = [IsAuthenticated]

    def get(self, request):
        """Get all validation rules."""
        try:
            validation_rules = ValidationRules.objects.all().order_by('id')
            serializer = ValidationRulesSerializer(validation_rules, many=True)
            return Response({"validation_rules": serializer.data})
        except Exception as e:
            return Response({
                "error": f"error: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def post(self, request):
        """Generate regex from selected validation rules."""
        try:
            selected_ids = request.data.get('selected_ids', [])
            request.data.get('max_length', None)

            if not selected_ids:
                return Response({
                    "error": "No rules selected"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Get selected rules from database
            selected_rules = ValidationRules.objects.filter(id__in=selected_ids, category__in=['first char', 'content']).order_by('id')

            # Build regex by combining expressions
            regex_parts = ['^']

            for rule in selected_rules:
                if rule.expression:
                    regex_parts.append(rule.expression)

            last_char_rules = ValidationRules.objects.filter(id__in=selected_ids, category='last char').order_by('id')
            if last_char_rules.exists():
                last_char_rule = last_char_rules.first()
                if last_char_rule and last_char_rule.expression:
                    regex_parts.append(last_char_rule.expression)
            else:
                # Add character class and end
                regex_parts.append('.+$')

            # Combine all parts
            final_regex = "r'" + ''.join(regex_parts) + "'"

            # Print to terminal
            print("\n" + "="*80)
            print("GENERATED REGULAR EXPRESSION")
            print("="*80)
            print(f"Regex: {final_regex}")
            print("\nSelected Validation Rules:")
            for rule in selected_rules:
                print(f"  - {rule.question}: {rule.expression}")
            print("="*80 + "\n")

            return Response({"regex": final_regex})

        except Exception as e:
            print(f"Error generating regex: {e!s}")
            return Response({
                "error": f"Failed to generate regex: {e!s}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
