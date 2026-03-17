from django.contrib import admin
from django.contrib.admin.models import LogEntry
from django.contrib.auth.admin import UserAdmin

from api.models import Country, Customer, Roles, SourceDB, SourceForm, User, UsrRoles, ValidationRules


class UserAdmin(UserAdmin):
    list_display = (
        'email',
        'first_name',
        'last_name',
        'created_on',
        'modified_on',
        'modified_by',
        'is_active',
        'is_staff',
        'is_superuser',
    )
    ordering = ('email',)  # Order by email instead of username

    # Add cust_id to the fieldsets for the add form
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('email', 'first_name', 'last_name', 'cust_id', 'password1', 'password2'),
        }),
    )

    # Add cust_id to the fieldsets for the change form
    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        ('Personal info', {'fields': ('first_name', 'last_name', 'cust_id')}),
        (
            'Permissions',
            {
                'fields': (
                    'is_active',
                    'is_staff',
                    'is_superuser',
                    'groups',
                    'user_permissions',
                ),
            },
        ),
        ('Important dates', {'fields': ('last_login', 'date_joined')}),
        ('Custom fields', {'fields': ( 'created_on', 'modified_by')}),
    )

# Custom LogEntry admin to handle string primary keys
class CustomLogEntryAdmin(admin.ModelAdmin):
    list_display = ('action_time', 'user_email', 'content_type', 'object_repr', 'action_flag', 'change_message')
    list_filter = ('action_time', 'action_flag', 'content_type')
    search_fields = ('user__email', 'object_repr', 'change_message')
    date_hierarchy = 'action_time'

    def user_email(self, obj):
        return obj.user.email if obj.user else 'Unknown'
    user_email.short_description = 'User Email'

# Register the custom LogEntry admin
admin.site.register(LogEntry, CustomLogEntryAdmin)

admin.site.register(User, UserAdmin)
admin.site.register(Customer)
admin.site.register(Country)
admin.site.register(SourceDB)
admin.site.register(SourceForm)
admin.site.register(ValidationRules)
admin.site.register(Roles)
admin.site.register(UsrRoles)
