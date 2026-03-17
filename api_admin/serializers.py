from rest_framework import serializers

from api.models import Country, Customer, User


class CountrySerializer(serializers.ModelSerializer):
    class Meta:
        model = Country
        fields = ['id', 'country_id', 'name']

class CustomerSerializer(serializers.ModelSerializer):
    country_name = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Customer
        fields = ['id',
            'cust_id', 'name', 'street1', 'street2', 'city', 'region',
            'country', 'country_name', 'phone', 'cust_db','created_by',
            'created_on', 'modified_on', 'modified_by', 'active'
        ]
        read_only_fields = ['cust_id', 'cust_db', 'created_on', 'modified_on', 'created_by', 'country_name']

    def get_country_name(self, obj):
        return obj.country.name if obj.country else None

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id','email', 'first_name', 'last_name', 'cust_id', 'created_by', 'created_on', 'modified_on', 'modified_by', 'is_active']
        read_only_fields = ['created_on', 'modified_on', 'created_by', 'modified_by']

    def create(self, validated_data):
        user= User(**validated_data)
        user.set_password("defaultpassword")
        user.save()
        return user
