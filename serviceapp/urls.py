from django.urls import path
from serviceapp.views import AmoCRMWebhookView, BalanceDepositConfirmView, BalanceDepositView, ClientRequestInfoView, ClientRequestsView, MasterFreeRequestsView, MasterStatisticsView, MasterStatsView, ServiceEquipmentTypesView, UserRegistrationView, ServiceRequestCreateView, ServiceRequestHistoryView, MasterActiveRequestsView, AssignRequestView, CloseRequestView, UserProfileView, FinishRequestView

urlpatterns = [
    path('register/', UserRegistrationView.as_view(), name='user_registration'),
    path('create_request/', ServiceRequestCreateView.as_view(), name='create_request'),
    path('requests_history/', ServiceRequestHistoryView.as_view(), name='requests_history'),
    path('master_active_requests/', MasterActiveRequestsView.as_view(), name='master_active_requests'),
    path('master_free_requests/', MasterFreeRequestsView.as_view(), name='master_free_requests'),
    path('assign_request/', AssignRequestView.as_view(), name='assign_request'),
    path('close_request/', CloseRequestView.as_view(), name='close_request'),
    path('finish_request/', FinishRequestView.as_view(), name='finish_request'),
    path('profile/', UserProfileView.as_view(), name='user_profile'),
    path('types/', ServiceEquipmentTypesView.as_view(), name='service_equipment_types'),
    path('amocrm-webhook/', AmoCRMWebhookView.as_view(), name='amocrm_webhook'),
    path('master_statistics/', MasterStatisticsView.as_view(), name='master_statistics'),
    path('client_requests/', ClientRequestsView.as_view(), name='client_requests'),
    path('client_request_info/', ClientRequestInfoView.as_view(), name = 'client_request_info'),
    path('master_stats/', MasterStatsView.as_view(), name='master_stats'),
    path('balance_deposit/', BalanceDepositView.as_view(), name='balance_deposit'),
    path('balance_deposit_confirm/', BalanceDepositConfirmView.as_view(), name='balance_deposit_confirm')
]
