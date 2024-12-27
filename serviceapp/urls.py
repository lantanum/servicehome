from django.urls import path
from serviceapp.views import AmoCRMWebhookView, ServiceEquipmentTypesView, UserRegistrationView, ServiceRequestCreateView, ServiceRequestHistoryView, MasterActiveRequestsView, AssignRequestView, CloseRequestView, UserProfileView

urlpatterns = [
    path('register/', UserRegistrationView.as_view(), name='user_registration'),
    path('create_request/', ServiceRequestCreateView.as_view(), name='create_request'),
    path('requests_history/', ServiceRequestHistoryView.as_view(), name='requests_history'),
    path('master_active_requests/', MasterActiveRequestsView.as_view(), name='master_active_requests'),
    path('assign_request/', AssignRequestView.as_view(), name='assign_request'),
    path('close_request/', CloseRequestView.as_view(), name='close_request'),
    path('profile/', UserProfileView.as_view(), name='user_profile'),
    path('types/', ServiceEquipmentTypesView.as_view(), name='service_equipment_types'),
    path('amocrm-webhook/', AmoCRMWebhookView.as_view(), name='amocrm_webhook'),
]
