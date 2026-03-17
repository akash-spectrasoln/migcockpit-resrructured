from django.urls import path

from .views import (
    CountryListView,
    CreateCustomerView,
    CreateUserView,
    CustomerListView,
    CustomerUserUpdateView,
    LoginView,
    LogoutView,
    PasswordResetConfirmView,
    PasswordResetView,
    Sessioncheckview,
    UserListView,
)

urlpatterns = [
    path('customer-create/', CreateCustomerView.as_view()),
    path('countries/', CountryListView.as_view(), name='country-list'),
    path('login/', LoginView.as_view(), name='admin-login-api'),
    path('logout/', LogoutView.as_view(), name='admin-logout'),
    path('session-check/', Sessioncheckview.as_view(), name='session-check'),
    path('api-customerslist/', CustomerListView.as_view(), name='api-customer-list'),
    path('api-create-user/', CreateUserView.as_view(), name='api-create-user'),
    path('api-userslist/', UserListView.as_view(), name='api-user-list'),
    path('password-reset/', PasswordResetView.as_view(), name='password-reset'),
    path('password-reset-confirm/', PasswordResetConfirmView.as_view(), name='password-reset-confirm'),
    path('customer-user-update/<int:id>/', CustomerUserUpdateView.as_view(), name='customer-user-update'),
]

from .frontendviews import (
    AdminDashboardView,
    AdminLoginView,
    CreateCustomerFormView,
    CreateUserFormView,
    CustomerListpageView,
    EditCustomerFormView,
    PasswordResetConfirmFormView,
    PasswordResetRequestView,
    UserListpageView,
)

frontend_urlpatterns = [
    path('admin-login/', AdminLoginView.as_view(), name='admin-login'),
    path('admin-dashboard/', AdminDashboardView.as_view(), name='admin-dashboard'),
    path('create-customer/', CreateCustomerFormView.as_view(), name='create-customer-form'),
    path('edit-customer/', EditCustomerFormView.as_view(), name='edit-customer-form'),
    path('customerslist/', CustomerListpageView.as_view(), name='customer-list'),
    path('userslist/', UserListpageView.as_view(), name='user-list'),
    path('create-user/', CreateUserFormView.as_view(), name='create-user-form'),
    path('password-reset-request/', PasswordResetRequestView.as_view(), name='password-reset-request'),
    path('reset-password/<str:uid>/<str:token>/', PasswordResetConfirmFormView.as_view(), name='password-reset-confirm-form'),
]

urlpatterns = urlpatterns + frontend_urlpatterns
