from django.urls import path
from serviceapp.views import UserRegistrationView

urlpatterns = [
    path('register/', UserRegistrationView.as_view(), name='user_registration'),
]
