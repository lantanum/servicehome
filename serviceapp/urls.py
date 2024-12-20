from django.urls import path
from serviceapp.views import UserRegistrationView, ServiceRequestCreateView,ServiceRequestHistoryView

urlpatterns = [
    path('register/', UserRegistrationView.as_view(), name='user_registration'),
    path('create_request/', ServiceRequestCreateView.as_view(), name='create_request'),
    path('requests_history/', ServiceRequestHistoryView.as_view(), name='requests_history'),
]
