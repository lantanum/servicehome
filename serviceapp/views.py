from datetime import timezone
from math import ceil
from django.utils.timezone import now, timedelta
from decimal import Decimal
import logging
import re
import threading
from django.conf import settings
from django.http import JsonResponse
import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from decimal import Decimal, ROUND_HALF_UP
from django.db.models import Sum, Avg, Q
from django.utils import timezone


from serviceapp.amocrm_client import AmoCRMClient
from serviceapp.utils import STATUS_MAPPING, parse_nested_form_data, MASTER_LEVEL_MAPPING
from .serializers import (
    AmoCRMWebhookSerializer,
    MasterStatisticsRequestSerializer,
    MasterStatisticsResponseSerializer,
    ServiceTypeSerializer,
    UserRegistrationSerializer, 
    ServiceRequestCreateSerializer, 
    RequestHistorySerializer, 
    ServiceRequestSerializer, 
    MasterActiveRequestsSerializer, 
    AssignRequestSerializer, 
    CloseRequestSerializer, 
    UserProfileRequestSerializer, 
    UserProfileSerializer
)
from .models import EquipmentType, Master, RatingLog, ReferralLink, ServiceRequest, ServiceType, Settings, Transaction, User, WorkOutcome

logger = logging.getLogger(__name__)

class UserRegistrationView(APIView):
    @swagger_auto_schema(
        operation_description="Регистрация пользователя или мастера.",
        request_body=UserRegistrationSerializer,
        responses={
            201: openapi.Response(
                description="Регистрация успешна",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING, description='Сообщение об успешной регистрации')
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING))
                        # Добавьте другие поля ошибок, если необходимо
                    }
                )
            )
        }
    )
    def post(self, request):
        serializer = UserRegistrationSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({"detail": "Registration successful"}, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class ServiceRequestCreateView(APIView):
    """
    API-эндпоинт для создания заявки клиентом.
    """
    @swagger_auto_schema(
        operation_description="Создание новой заявки клиентом.",
        request_body=ServiceRequestCreateSerializer,
        responses={
            201: openapi.Response(
                description="Заявка успешно создана",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING, description='Сообщение об успешном создании заявки'),
                        'request_id': openapi.Schema(type=openapi.TYPE_INTEGER, description='ID созданной заявки')
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING))
                        # Добавьте другие поля ошибок, если необходимо
                    }
                )
            )
        }
    )
    def post(self, request):
        serializer = ServiceRequestCreateSerializer(data=request.data)
        if serializer.is_valid():
            service_request = serializer.save()
            return Response({
                "detail": "Заявка успешно создана",
                "request_id": service_request.amo_crm_lead_id
            }, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class ServiceRequestHistoryView(APIView):
    @swagger_auto_schema(
        operation_description="Получение истории заявок клиента по его telegram_id.",
        request_body=RequestHistorySerializer,
        responses={
            200: openapi.Response(
                description="Успешный ответ с историей заявок",
                schema=ServiceRequestSerializer(many=True)
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING))
                    }
                )
            ),
            403: openapi.Response(
                description="Доступ запрещен",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Пользователь не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        serializer = RequestHistorySerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        telegram_id = serializer.validated_data['telegram_id']

        # Проверяем, что пользователь с таким telegram_id существует и является клиентом
        try:
            user = User.objects.get(telegram_id=telegram_id)
        except User.DoesNotExist:
            return Response({"detail": "Пользователь с указанным telegram_id не найден."}, 
                            status=status.HTTP_404_NOT_FOUND)

        if user.role != 'Client':
            return Response({"detail": "Историю заявок можно просматривать только для клиентов."}, 
                            status=status.HTTP_403_FORBIDDEN)

        # Получаем заявки данного клиента
        requests_qs = ServiceRequest.objects.filter(client=user).order_by('-created_at')

        # Сериализуем заявки
        sr_serializer = ServiceRequestSerializer(requests_qs, many=True)
        return Response(sr_serializer.data, status=status.HTTP_200_OK)


class MasterActiveRequestsView(APIView):
    """
    API-эндпоинт для получения активных заявок мастера по telegram_id.
    """
    @swagger_auto_schema(
        operation_description="Получение активных заявок мастера по его telegram_id.",
        request_body=MasterActiveRequestsSerializer,
        responses={
            200: openapi.Response(
                description="Успешный ответ с сообщениями об активных заявках",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "request_1": openapi.Schema(
                            type=openapi.TYPE_OBJECT,
                            properties={
                                "message_text": openapi.Schema(type=openapi.TYPE_STRING),
                                "finish_button_text": openapi.Schema(type=openapi.TYPE_STRING)
                            }
                        ),
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Items(type=openapi.TYPE_STRING)
                        )
                    }
                )
            ),
            403: openapi.Response(
                description="Доступ запрещен",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Мастер не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        serializer = MasterActiveRequestsSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        telegram_id = serializer.validated_data['telegram_id']

        # Поиск пользователя по telegram_id
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response({"detail": "Пользователь с указанным telegram_id не найден."}, 
                            status=status.HTTP_404_NOT_FOUND)

        # Проверка роли
        if user.role != 'Master':
            return Response({"detail": "Доступно только для пользователей с ролью 'Master'."}, 
                            status=status.HTTP_403_FORBIDDEN)

        # Получаем профиль мастера
        try:
            master = user.master_profile  # или user.master
        except AttributeError:
            return Response({"detail": "Мастер не найден для данного пользователя."},
                            status=status.HTTP_404_NOT_FOUND)

        # Получаем активные заявки, включая QualityControl
        active_requests = ServiceRequest.objects.filter(
            master=master,
            status__in=['In Progress', 'AwaitingClosure', 'QualityControl']
        ).order_by('-created_at')[:10]

        # Если заявок нет
        if not active_requests:
            return Response(
                {
                    "request_1": {
                        "message_text": "🥳Нет активных заявок!",
                        "finish_button_text": ""
                    }
                },
                status=status.HTTP_200_OK
            )

        # Собираем ответ в формате request_1, request_2, ...
        result = {}
        for i, req in enumerate(active_requests):
            field_name = f"request_{i+1}"

            if req.status == 'QualityControl':
                # Текст для заявок со статусом QualityControl
                message_text = (
                    f"Заявка под номером {req.amo_crm_lead_id or req.id} находится на стадии проверки "
                    f"у службы контроля качества."
                )
                finish_button_text = ""  # Для этого статуса кнопка не требуется
            else:
                # Текст для остальных заявок
                date_str = req.created_at.strftime('%d.%m.%Y') if req.created_at else ""
                message_text = (
                    f"<b>Заявка</b> {req.amo_crm_lead_id}\n"
                    f"<b>Дата заявки:</b> {date_str} г.\n"
                    f"<b>Город:</b> {req.city_name or ''}\n"
                    f"<b>Адрес:</b> {req.address or ''}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    f"<b>Имя:</b> {req.client.name}\n"
                    f"<b>Телефон:</b> {req.client.phone}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    f"<b>Тип оборудования:</b> {req.equipment_type or ''}\n"
                    f"<b>Марка:</b> {req.equipment_brand or ''}\n"
                    f"<b>Модель:</b> {req.equipment_model or '-'}\n"
                    f"<b>Комментарий:</b> {req.description or ''}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    "Бесплатный выезд и диагностика* - Бесплатный выезд и диагностика "
                    "только при оказании ремонта. ВНИМАНИЕ! - В случае отказа от ремонта "
                    "- Диагностика и выезд платные (Цену формирует мастер)."
                )
                finish_button_text = f"Сообщить о завершении {req.amo_crm_lead_id}"

            result[field_name] = {
                "message_text": message_text,
                "finish_button_text": finish_button_text
            }

        return Response(result, status=status.HTTP_200_OK)


class AssignRequestView(APIView):
    """
    API-эндпоинт для того, чтобы мастер мог взять заявку в работу по её ID.
    """
    @swagger_auto_schema(
        operation_description="Мастер берет заявку в работу по её ID.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'telegram_id': openapi.Schema(type=openapi.TYPE_STRING, description="Telegram ID мастера"),
                'request_id': openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки (внутренний)"),
            },
            required=['telegram_id', 'request_id']
        ),
        responses={
            200: openapi.Response(
                description="Заявка успешно взята в работу",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'message_for_master': openapi.Schema(type=openapi.TYPE_STRING),
                        'message_for_admin': openapi.Schema(type=openapi.TYPE_STRING),
                        'finish_button_text': openapi.Schema(type=openapi.TYPE_STRING),
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные или заявка уже назначена (детали в поле detail).",
            ),
            404: openapi.Response(
                description="Пользователь или заявка не найдены",
            ),
            500: openapi.Response(
                description="Внутренняя ошибка сервера",
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get('telegram_id')
        request_id = data.get('request_id')

        if not telegram_id or not request_id:
            return JsonResponse({'error': 'telegram_id and request_id are required'}, status=400)

        try:
            with transaction.atomic():
                # 1) Ищем мастера
                master_user = User.objects.select_for_update().get(telegram_id=telegram_id, role="Master")
                master = master_user.master_profile

                # (1) Проверка баланса
                if master.balance < 0:
                    return JsonResponse(
                        {"message_for_master": "У вас отрицательный баланс, пополните баланс, чтобы продолжить получать заявки"},
                        status=200
                    )

                # 2) Получаем настройки (лимиты заявок)
                settings_obj = Settings.objects.first()
                if not settings_obj:
                    max_req_l1, max_req_l2, max_req_l3 = 1, 3, 5
                else:
                    max_req_l1 = settings_obj.max_requests_level1
                    max_req_l2 = settings_obj.max_requests_level2
                    max_req_l3 = settings_obj.max_requests_level3

                level = master.level or 1
                if level == 1:
                    max_requests = max_req_l1
                elif level == 2:
                    max_requests = max_req_l2
                elif level == 3:
                    max_requests = max_req_l3
                else:
                    max_requests = 9999

                # (2) Проверка лимита заявок (In Progress)
                active_count = ServiceRequest.objects.filter(
                    master=master,
                    status__in=['In Progress', 'AwaitingClosure', 'QualityControl']
                ).count()
                if active_count >= max_requests:
                    return JsonResponse(
                        {
                            "message_for_master": (
                                "У вас уже есть активные заявки, сначала завершите их.\n"
                                "Чтобы увидеть заявки в работе, нажмите кнопку «Заявки в работе»."
                            )
                        },
                        status=200
                    )

                # 3) Находим заявку
                service_request = ServiceRequest.objects.select_for_update().get(amo_crm_lead_id=request_id)
                original_status = service_request.status

                # (3) Проверка, свободна ли заявка
                if original_status != 'Free':
                    return JsonResponse(
                        {"message_for_master": "Данную заявку уже выполняет другой мастер"},
                        status=200
                    )

                # ---- Если все проверки пройдены, переводим заявку в работу ----
                service_request.master = master
                service_request.status = 'In Progress'
                service_request.start_date = timezone.now()
                service_request.save()

                # Обновляем сделку в amoCRM
                lead_id = service_request.amo_crm_lead_id
                if not lead_id or not master_user.amo_crm_contact_id:
                    return JsonResponse(
                        {'error': 'AmoCRM IDs for request or master are missing'},
                        status=400
                    )
                amocrm_client = AmoCRMClient()
                category_service = master.service_name or ""
                equipment_type_value = master.equipment_type_name or ""

                amocrm_client.update_lead(
                    lead_id,
                    {
                        "status_id": STATUS_MAPPING["In Progress"],
                        "custom_fields_values": [
                            {
                                "field_id": 748205,
                                "values": [{"value": category_service}]
                            },
                            {
                                "field_id": 748321,
                                "values": [{"value": equipment_type_value}]
                            },
                            {
                                "field_id": 748327,
                                "values": [{"value": "подходящее значение"}]
                            },
                            {
                                "field_id": 748213,
                                "values": [{"value": "подходящее значение"}]
                            },
                            {
                                "field_id": 748329,
                                "values": [{"value": str(master.balance)}]
                            }
                        ]
                    }
                )
                amocrm_client.attach_contact_to_lead(lead_id, master_user.amo_crm_contact_id)

                # Формируем три нужных поля: два сообщения и текст кнопки
                created_date_str = (
                    service_request.created_at.strftime('%d.%m.%Y')
                    if service_request.created_at
                    else None
                )
                city_name = service_request.city_name or ""
                raw_address = service_request.address or ""
                client_user = service_request.client
                amo_id = lead_id or service_request.id

                message_for_master = (
                    f"<b>Заявка</b> {amo_id}\n"
                    f"<b>Дата заявки:</b> {created_date_str}\n"
                    f"<b>Город:</b> {city_name}\n"
                    f"<b>Адрес:</b> {raw_address}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    f"<b>Имя:</b> {client_user.name}\n"
                    f"<b>Тел.:</b> {client_user.phone}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    f"<b>Тип оборудования:</b> {service_request.equipment_type or ''}\n"
                    f"<b>Марка:</b> {service_request.equipment_brand or ''}\n"
                    f"<b>Модель:</b> {service_request.equipment_model or ''}\n"
                    f"<b>Комментарий:</b> {service_request.description or ''}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    "Бесплатный выезд и диагностика* - Бесплатный выезд и диагностика "
                    "только при оказании ремонта. ВНИМАНИЕ! - В случае отказа от ремонта "
                    "- Диагностика и выезд платные (Цену формирует мастер)."
                )

                message_for_admin = (
                    f"<b>Заявка</b> {service_request.amo_crm_lead_id}\n"
                    f"<b>Дата заявки:</b> {created_date_str}\n"
                    f"<b>Город:</b> {city_name}\n"
                    f"<b>Адрес:</b> {raw_address}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    f"<b>Имя:</b> {client_user.name}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                    f"<b>Тип оборудования:</b> {service_request.equipment_type or ''}\n"
                    f"<b>Комментарий:</b> {service_request.description or ''}\n"
                    "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n\n"
                    f"<b>Взял мастер</b> {master_user.name}\n"
                    f"{master_user.phone}\n"
                    f"<b>ID</b> = {telegram_id}"
                )

                finish_button_text = f"Сообщить о завершении {amo_id}"

                # Отдаём три поля в JSON
                response_data = {
                    "message_for_master": message_for_master,
                    "message_for_admin": message_for_admin,
                    "finish_button_text": finish_button_text,
                    "client_telegram_id": client_user.telegram_id,
                    "request_id": service_request.amo_crm_lead_id
                }
                return JsonResponse(response_data, status=200)

        except User.DoesNotExist:
            return JsonResponse(
                {"detail": "Пользователь с указанным telegram_id не найден."},
                status=404
            )
        except ServiceRequest.DoesNotExist:
            return JsonResponse(
                {"detail": "Заявка с указанным ID не найдена."},
                status=404
            )
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return JsonResponse(
                {"detail": "Произошла ошибка при присвоении заявки."},
                status=500
            )


class CloseRequestView(APIView):
    """
    API-эндпоинт для закрытия заявки мастером.
    """
    @swagger_auto_schema(
        operation_description="Закрытие заявки мастером.",
        request_body=CloseRequestSerializer,
        responses={
            200: openapi.Response(
                description="Заявка успешно закрыта",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING, description='Сообщение об успешном закрытии заявки')
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING))
                    }
                )
            ),
            404: openapi.Response(
                description="Пользователь или заявка не найдены",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            500: openapi.Response(
                description="Внутренняя ошибка сервера",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        serializer = CloseRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            service_request = serializer.save()
        except Exception as e:
            return Response(
                {"detail": "Произошла ошибка при закрытии заявки."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        return Response(
            {"detail": "Заявка успешно закрыта."},
            status=status.HTTP_200_OK
        )
        

def get_referral_count_1_line(user: User) -> int:
    """
    Количество рефералов, которых пригласил текущий user напрямую.
    Т.е. count() всех ReferralLink, у которых referrer_user == user.
    """
    return user.referrer_links.count()
def get_referral_count_2_line(user: User) -> int:
    """
    Количество "внуков" – тех, у кого referrer_user == (кто-то из 1-й линии).
    """
    count_2_line = 0
    # Все прямые рефералы (1-я линия)
    first_line = user.referrer_links.all()  # QuerySet ReferralLink, где referrer_user=user

    # Для каждого ReferralLink из first_line, возьмём referred_user
    # и посмотрим, сколько у него есть "referrer_links" (т. е. 1-я линия для него).
    for link in first_line:
        child_user = link.referred_user
        count_2_line += child_user.referrer_links.count()
    return count_2_line



class UserProfileView(APIView):
    """
    API-эндпоинт для получения профиля пользователя по telegram_id.
    """
    @swagger_auto_schema(
        operation_description="Получение профиля пользователя по его telegram_id.",
        request_body=UserProfileRequestSerializer,
        responses={
            200: openapi.Response(
                description="Успешный ответ с данными профиля пользователя",
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING))
                    }
                )
            ),
            404: openapi.Response(
                description="Пользователь не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        serializer = UserProfileRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        telegram_id = serializer.validated_data['telegram_id']
        
        try:
            # Просто находим пользователя (клиента или мастера) по telegram_id
            user = User.objects.get(telegram_id=telegram_id, role='Client')
        except User.DoesNotExist:
            return Response({"detail": "Пользователь с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)

        # Подсчёт рефералов
        count_1_line = get_referral_count_1_line(user)
        count_2_line = get_referral_count_2_line(user)
        total_referrals = count_1_line + count_2_line

        # Формируем ответ (city, name, phone, balance, daily_income, level и т.д.)
        response_data = {
            "city": user.city_name or "",
            "name": user.name or "",
            "phone": user.phone or "",
            "balance": str(int(user.balance)),
            "daily_income": "0",   # заглушка, поменяйте под логику
            "level": user.level,          # заглушка
            "referral_count": total_referrals,
            "referral_count_1_line": count_1_line,
            "referral_count_2_line": count_2_line
        }

        return Response(response_data, status=status.HTTP_200_OK)

class ServiceEquipmentTypesView(APIView):
    """
    API-эндпоинт для получения списка типов сервисов и их вложенных типов оборудования.
    """
    @swagger_auto_schema(
        operation_description="Получение списка типов сервисов, внутри каждого - его типы оборудования.",
        responses={
            200: openapi.Response(
                description="Список типов сервисов со вложенными типами оборудования",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "service_types": openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Items(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    
                                    "name": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="Название типа сервиса"
                                    ),
                                    "equipment_types": openapi.Schema(
                                        type=openapi.TYPE_ARRAY,
                                        items=openapi.Items(
                                            type=openapi.TYPE_OBJECT,
                                            properties={
                                                "name": openapi.Schema(
                                                    type=openapi.TYPE_STRING,
                                                    description="Название типа оборудования"
                                                )
                                            }
                                        )
                                    )
                                }
                            )
                        )
                    }
                )
            )
        }
    )
    def post(self, request):
        service_types = ServiceType.objects.all()
        
        # Используем уже созданный сериализатор
        serializer = ServiceTypeSerializer(service_types, many=True)

        # Возвращаем в формате {"service_types": [{...}, {...}]}
        return Response({
            "service_types": serializer.data
        }, status=status.HTTP_200_OK)

    

def extract_street_name(address):
    """
    Извлекает название улицы из полного адреса.
    Например, из "Ленина 12" возвращает "Ленина".
    """
    # Используем регулярное выражение для извлечения текста до первой цифры
    match = re.match(r'^(.+?)\s+\d+', address)
    if match:
        return match.group(1)
    else:
        # Если не удалось найти цифру, возвращаем полный адрес
        return address.strip()

def format_date(created_at):
    """
    Форматирует дату в формате "день месяц год", где месяц - название на русском.
    Например, из "2024-12-28T12:34:56Z" возвращает "28 декабря 2024".
    """
    month_names = {
        1: 'января',
        2: 'февраля',
        3: 'марта',
        4: 'апреля',
        5: 'мая',
        6: 'июня',
        7: 'июля',
        8: 'августа',
        9: 'сентября',
        10: 'октября',
        11: 'ноября',
        12: 'декабря'
    }
    day = created_at.day
    month = month_names.get(created_at.month, '')
    year = created_at.year
    return f"{day} {month} {year}"


def recalc_master_rating(master):
    """
    Новый вариант пересчёта рейтинга мастера:
    Учитываем только заявки со статусом Completed,
    заполненными 3 полями рейтинга клиента
    и ссылкой на WorkOutcome, у которого outcome_rating не null.

    Итог за заявку = (среднее трёх клиентских рейтингов + outcome_rating) / 2
    Среднее по всем таким заявкам = общий рейтинг мастера.
    """
    requests_qs = ServiceRequest.objects.filter(
        master=master,
        status='Completed',
        quality_rating__isnull=False,
        competence_rating__isnull=False,
        recommendation_rating__isnull=False,
        work_outcome__isnull=False,               # Вместо work_outcome_record__isnull=False
        work_outcome__outcome_rating__isnull=False
    )

    total = Decimal('0.0')
    count = 0

    for req in requests_qs:
        # Среднее трёх клиентских рейтингов
        client_avg = (req.quality_rating + req.competence_rating + req.recommendation_rating) / 3
        client_avg_dec = Decimal(client_avg)

        # Рейтинг исхода работы из справочника
        outcome_rating_dec = Decimal(req.work_outcome.outcome_rating)

        # Итоговое значение заявки
        final_req_rating = (client_avg_dec + outcome_rating_dec) / Decimal('2.0')

        total += final_req_rating
        count += 1

    final_master_rating = total / count if count > 0 else Decimal('0.0')
    master.rating = final_master_rating
    master.save(update_fields=['rating'])

def update_commission_transaction(service_request, new_price):
    """
    Вычисляет новую комиссию по новому значению цены и суммирует уже начисленные транзакции комиссии для данной заявки.
    Если новая комиссия больше уже начисленной, создаётся дополнительная транзакция на разницу.
    Функция возвращает разницу (положительное число) или None, если дополнительное списание не требуется.
    """
    new_price_value = Decimal(new_price)
    master_profile = service_request.master
    if not master_profile:
        return None
    master_level = master_profile.level

    service_type = None
    if service_request.service_name:
        service_type = ServiceType.objects.filter(name=service_request.service_name).first()
    if not service_type:
        commission_percentage = Decimal('0.0')
        logger.warning(
            f"ServiceRequest {service_request.id}: ServiceType with name='{service_request.service_name}' not found. Commission = 0 by default."
        )
    else:
        if master_level == 1:
            commission_percentage = service_type.commission_level_1 or Decimal('0.0')
        elif master_level == 2:
            commission_percentage = service_type.commission_level_2 or Decimal('0.0')
        elif master_level == 3:
            commission_percentage = service_type.commission_level_3 or Decimal('0.0')
        else:
            commission_percentage = Decimal('0.0')
    spare_parts = service_request.spare_parts_spent or Decimal('0.0')
    deal_amount = new_price_value - spare_parts
    new_commission_amount = deal_amount * commission_percentage / Decimal('100')

    # Суммируем уже созданные транзакции комиссии для этой заявки
    old_commission_agg = Transaction.objects.filter(
        service_request=service_request,
        transaction_type='Comission'
    ).aggregate(total=Sum('amount'))
    old_commission_total = old_commission_agg['total'] or Decimal('0.0')

    difference = new_commission_amount - old_commission_total
    if difference > Decimal('0.0'):
        Transaction.objects.create(
            master = master_profile,
            amount=difference,
            transaction_type='Comission',
            status='Confirmed',
            service_request=service_request
        )
        logger.info(
            f"Additional commission transaction created for ServiceRequest {service_request.id}: difference = {difference}"
        )
        return difference
    else:
        logger.info(
            f"No additional commission transaction required for ServiceRequest {service_request.id}. Old commission: {old_commission_total}, New commission: {new_commission_amount}"
        )
        return None

class AmoCRMWebhookView(APIView):
    """
    API-эндпоинт для приема вебхуков от AmoCRM о статусах лидов.
    """
    def post(self, request):
        try:
            raw_data = request.body.decode('utf-8')
            logger.debug(f"Incoming AmoCRM webhook raw data: {raw_data}")
        except Exception as e:
            logger.error(f"Error decoding request body: {e}")
            return Response({"detail": "Invalid request body."}, status=400)

        nested_data = parse_nested_form_data(request.POST)
        logger.debug(f"Parsed AmoCRM webhook data: {nested_data}")

        serializer = AmoCRMWebhookSerializer(data=nested_data)
        if not serializer.is_valid():
            logger.warning(f"Invalid AmoCRM webhook data: {serializer.errors}")
            return Response(serializer.errors, status=400)

        embedded = serializer.validated_data.get('leads', {})
        status_changes = embedded.get('status', [])

        for lead in status_changes:
            try:
                lead_id = lead.get('id')
                new_status_id = lead.get('status_id')
                operator_comment = lead.get('748437', "")
                deal_success = lead.get('748715', "")
                quality_rating = lead.get('748771')         # Качество работ
                competence_rating = lead.get('748773')        # Компетентность мастера
                recommendation_rating = lead.get('748775')    # Готовность рекомендовать
                incoming_price = lead.get('price')            # Приходящее значение цены (строка)
                work_outcome_name = lead.get('745353')          # Название итога работы
        
                # Определяем новое имя статуса по STATUS_MAPPING
                status_name = None
                for k, v in STATUS_MAPPING.items():
                    if v == new_status_id:
                        status_name = k
                        break
                if not status_name:
                    logger.warning(f"No matching status found for status_id={new_status_id}")
                    status_name = 'Open'  # либо другой статус по умолчанию
        
                with transaction.atomic():
                    try:
                        # Пытаемся найти существующую заявку
                        service_request = ServiceRequest.objects.select_for_update().get(
                            amo_crm_lead_id=lead_id
                        )
                        # Обновляем базовые поля заявки
                        service_request.crm_operator_comment = operator_comment
                        service_request.deal_success = deal_success
                        if quality_rating is not None:
                            service_request.quality_rating = int(quality_rating)
                        if competence_rating is not None:
                            service_request.competence_rating = int(competence_rating)
                        if recommendation_rating is not None:
                            service_request.recommendation_rating = int(recommendation_rating)
        
                    except ServiceRequest.DoesNotExist:
                        # Если заявка не найдена – создаём новую
                        phone = lead.get('phone')
                        telegram_id = lead.get('telegram_id')
        
                        # Ищем пользователя по номеру телефона или telegram_id
                        user = None
                        if phone or telegram_id:
                            user = User.objects.filter(Q(phone=phone) | Q(telegram_id=telegram_id)).first()
        
                        if user:
                            if user.role == 'Master':
                                try:
                                    master = Master.objects.get(user=user)
                                except Master.DoesNotExist:
                                    logger.error(f"User {user.id} имеет роль 'Master', но профиль мастера не найден.")
                                    master = None
                            else:
                                master = None
                        else:
                            # Если пользователь не найден, создаём нового клиента
                            user = User.objects.create(
                                name=lead.get('name', 'Новый клиент'),
                                phone=phone,
                                telegram_id=telegram_id,
                                role='Client'
                            )
                            master = None
        
                        service_request = ServiceRequest.objects.create(
                            client=user,
                            master=master,
                            amo_crm_lead_id=lead_id,
                            status=status_name,
                            amo_status_code=new_status_id,
                            price=Decimal(incoming_price) if incoming_price is not None else None,
                            crm_operator_comment=operator_comment,
                            deal_success=deal_success,
                            quality_rating=int(quality_rating) if quality_rating is not None else None,
                            competence_rating=int(competence_rating) if competence_rating is not None else None,
                            recommendation_rating=int(recommendation_rating) if recommendation_rating is not None else None,
                        )
                        logger.info(f"Created new ServiceRequest with amo_crm_lead_id={lead_id}")
        
                    # Обрабатываем итог работы (work_outcome)
                    if work_outcome_name:
                        try:
                            outcome = WorkOutcome.objects.get(outcome_name=work_outcome_name)
                            service_request.work_outcome = outcome
                        except WorkOutcome.DoesNotExist:
                            logger.warning(f"WorkOutcome with name '{work_outcome_name}' not found for lead_id {lead_id}.")
        
                    previous_status = service_request.status
        
                    # Обрабатываем статусы: AwaitingClosure, Completed, QualityControl
                    if status_name in ['AwaitingClosure', 'Completed', 'QualityControl']:
                        if incoming_price is not None:
                            new_price_val = Decimal(incoming_price)
                            if service_request.price != new_price_val:
                                diff = update_commission_transaction(service_request, incoming_price)
                                service_request.price = new_price_val
                                if (service_request.master and service_request.master.user.telegram_id and diff is not None):
                                    payload = {
                                        "master_telegram_id": service_request.master.user.telegram_id,
                                        "message": f"С вас списана комиссия в размере {ceil(float(diff))} монет по заявке {service_request.amo_crm_lead_id}.\n\nВажно! Для того, чтобы получать новые заказы, необходимо иметь положительный баланс."
                                    }
                                    try:
                                        response_msg = requests.post('https://sambot.ru/reactions/2849416/start?token=yhvtlmhlqbj', json=payload, timeout=10)
                                        if response_msg.status_code != 200:
                                            logger.error(f"Failed to send commission info to sambot. Status code: {response_msg.status_code}, Response: {response_msg.text}")
                                    except Exception as ex:
                                        logger.error(f"Error sending commission info to sambot: {ex}")
                        # Обновляем поля заявки
                        service_request.status = status_name
                        service_request.amo_status_code = new_status_id
                        fields_to_update = ['status', 'amo_status_code']
                        if incoming_price is not None:
                            fields_to_update.append('price')
                        # Если итог работы был изменён – добавляем и его в список обновлений
                        if work_outcome_name:
                            fields_to_update.append('work_outcome')
                        service_request.save(update_fields=fields_to_update)
                        logger.info(f"ServiceRequest {service_request.id}: status updated from {previous_status} to '{status_name}' with updated price.")
        
                        if status_name == 'AwaitingClosure':
                            if service_request.master and service_request.master.user.telegram_id:
                                payload = {
                                    "telegram_id": service_request.master.user.telegram_id,
                                    "request_id": str(lead_id)
                                }
                                try:
                                    response = requests.post('https://sambot.ru/reactions/2939774/start?token=yhvtlmhlqbj', json=payload, timeout=10)
                                    if response.status_code != 200:
                                        logger.error(f"Failed to send data to sambot (AwaitingClosure) for Request {service_request.id}. Status: {response.status_code}, Response: {response.text}")
                                except Exception as ex:
                                    logger.error(f"Error sending data to sambot: {ex}")
                        elif status_name == 'Completed':
                            handle_completed_deal(
                                service_request=service_request,
                                operator_comment=operator_comment,
                                previous_status=previous_status,
                                lead_id=lead_id,
                                skip_commission=True
                            )
                    elif status_name == 'Free':
                        previous_status = service_request.status
                        handle_free_status(service_request, previous_status, new_status_id)
                    else:
                        logger.info(f"Ignoring status {status_name} (id={new_status_id}) for lead_id={lead_id}")
        
            except Exception as e:
                logger.exception(f"Error processing lead_id={lead_id}: {e}")
                continue

        return Response({"detail": "Webhook processed."}, status=200)

def handle_free_status(service_request, previous_status, new_status_id):
    """
    Обработка статуса 'Free' с 3-мя кругами рассылки.
    """
    service_request.status = 'Free'
    service_request.amo_status_code = new_status_id
    service_request.save()

    logger.info(f"[ServiceRequest {service_request.id}] Статус обновлён "
                f"с {previous_status} на 'Free'.")

    # 1-й круг (отправляется сразу)
    logger.info(f"[ServiceRequest {service_request.id}] Запуск 1-го круга рассылки.")
    masters_round_1 = find_suitable_masters(service_request.id, round_num=1)
    logger.info(f"[ServiceRequest {service_request.id}] Найдено {len(masters_round_1)} мастеров для 1-го круга.")
    send_request_to_sambot(service_request, masters_round_1, round_num=1)

    delay_2 = 60 if masters_round_1 else 0  # Если нет мастеров, сразу запускаем 2-й круг
    threading.Timer(delay_2, send_request_to_sambot_with_logging, [service_request.id, 2]).start()

def send_request_to_sambot_with_logging(service_request_id, round_num):
    """
    Функция-обертка для логирования перед отправкой запроса.
    """
    service_request = ServiceRequest.objects.get(id=service_request_id)

    logger.info(f"[ServiceRequest {service_request.id}] Запуск {round_num}-го круга рассылки.")
    masters = find_suitable_masters(service_request.id, round_num)
    logger.info(f"[ServiceRequest {service_request.id}] Найдено {len(masters)} мастеров для {round_num}-го круга.")

    send_request_to_sambot(service_request, masters, round_num)

    if round_num == 2:
        delay_3 = 60 if masters else 0  # Если нет мастеров во 2-м круге, сразу запускаем 3-й
        threading.Timer(delay_3, send_request_to_sambot_with_logging, [service_request.id, 3]).start()

def send_request_to_sambot(service_request, masters_telegram_ids, round_num):
    """
    Отправляет данные на Sambot.
    """
    if not masters_telegram_ids and round_num != 1:
        logger.info(f"[ServiceRequest {service_request.id}] Нет мастеров для отправки в этом круге.")
        return
    
    result = generate_free_status_data(service_request)

    payload = {
        "message_for_masters": result["message_for_masters"],
        "finish_button_text": result["finish_button_text"],
        "masters_telegram_ids": masters_telegram_ids,
        "round_num": round_num,
        "message_for_admin": result["message_for_admin"] if round_num == 1 else ""
    }

    try:
        response = requests.post(
            'https://sambot.ru/reactions/2890052/start?token=yhvtlmhlqbj',
            json=payload,
            timeout=10
        )
        if response.status_code == 200:
            logger.info(f"[ServiceRequest {service_request.id}] Успешно отправлено в Sambot.")
        else:
            logger.error(f"[ServiceRequest {service_request.id}] Ошибка при отправке данных в Sambot. "
                         f"Статус код: {response.status_code}, Ответ: {response.text}")
    except Exception as ex:
        logger.error(f"[ServiceRequest {service_request.id}] Ошибка при отправке данных в Sambot: {ex}")

def find_suitable_masters(service_request_id, round_num):
    """
    Подбирает мастеров для рассылки в зависимости от номера круга.
    Условия для кругов берутся из настроек (Settings).
    Если Settings не заданы, используются значения по умолчанию.
    """
    service_request = ServiceRequest.objects.get(id=service_request_id)

    city_name = service_request.city_name.lower()
    equipment_type = (service_request.equipment_type or "").lower()

    # Выбираем мастеров, у которых пользователь активен
    masters = Master.objects.select_related('user').filter(user__is_active=True)
    selected_masters = []

    now_time = now()
    last_24_hours = now_time - timedelta(hours=24)

    # Получаем настройки для кругов
    settings_obj = Settings.objects.first()
    if settings_obj:
        round1_success_ratio = settings_obj.round1_success_ratio or Decimal("0.8")
        round1_cost_ratio_max = settings_obj.round1_cost_ratio_max or Decimal("0.3")
        round2_success_ratio = settings_obj.round2_success_ratio or Decimal("0.8")
        round2_cost_ratio_min = settings_obj.round2_cost_ratio_min or Decimal("0.3")
        round2_cost_ratio_max = settings_obj.round2_cost_ratio_max or Decimal("0.5")
    else:
        round1_success_ratio, round1_cost_ratio_max = Decimal("0.8"), Decimal("0.3")
        round2_success_ratio, round2_cost_ratio_min, round2_cost_ratio_max = Decimal("0.8"), Decimal("0.3"), Decimal("0.5")

    for master in masters:
        master_cities = (master.city_name or "").lower()
        master_equips = (master.equipment_type_name or "").lower()

        if city_name in master_cities and equipment_type in master_equips:
            success_ratio, cost_ratio, last_deposit = get_master_statistics(master)
            if round_num == 1:
                if (success_ratio >= round1_success_ratio and
                    cost_ratio <= round1_cost_ratio_max and
                    last_deposit >= last_24_hours):
                    selected_masters.append(master.user.telegram_id)
            elif round_num == 2:
                if (success_ratio >= round2_success_ratio and
                    cost_ratio > round2_cost_ratio_min and
                    cost_ratio <= round2_cost_ratio_max):
                    selected_masters.append(master.user.telegram_id)
            elif round_num == 3:
                # Во 3‑й круг можно включить всех оставшихся (без дополнительных условий)
                selected_masters.append(master.user.telegram_id)

    return selected_masters



def get_master_statistics(master):
    """
    Возвращает статистику мастера:
    - success_ratio: доля успешных заявок
    - cost_ratio: доля затрат от всех заказов
    - last_deposit: время последнего пополнения
    """
    total_orders = master.master_requests.count()
    successful_orders = master.master_requests.filter(deal_success="Успешная сделка (Выполнено)").count()
    total_cost = sum(request.spare_parts_spent or 0 for request in master.master_requests.all())
    total_earnings = sum(request.price or 0 for request in master.master_requests.all())

    success_ratio = successful_orders / total_orders if total_orders > 0 else 0
    cost_ratio = total_cost / total_earnings if total_earnings > 0 else 0

    last_transaction = Transaction.objects.filter(
        master=master, transaction_type="Deposit", status="Confirmed"
    ).order_by("-created_at").first()

    last_deposit = last_transaction.created_at if last_transaction else now() - timedelta(days=365)

    return success_ratio, cost_ratio, last_deposit


def generate_free_status_data(service_request):
    """
    Генерирует данные (сообщения и список мастеров) для статуса 'Free'.
    """
    city_name = service_request.city_name or ""
    raw_address = service_request.address or ""
    created_date_str = (
        service_request.created_at.strftime('%d.%m.%Y')
        if service_request.created_at
        else ""
    )

    # Короткий адрес (первое слово из адреса)
    address_parts = raw_address.strip().split()
    short_address = address_parts[0] if address_parts else ""

    # Сообщение для мастеров
    message_for_masters = (
        f"<b>Город:</b> {city_name}\n"
        f"<b>Адрес:</b> {short_address}\n"
        f"<b>Дата заявки:</b> {created_date_str}\n"
        f"<b>Тип оборудования:</b> {service_request.equipment_type or ''}\n"
        f"<b>Марка:</b> {service_request.equipment_brand or ''}\n"
        f"<b>Модель:</b> {service_request.equipment_model or ''}\n"
        "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
        f"<b>Комментарий:</b> {service_request.description or ''}"
    )

    # Сообщение для администраторов
    message_for_admin = (
        f"<b>Заявка</b> {service_request.amo_crm_lead_id}\n"
        f"<b>Дата заявки:</b> {created_date_str}\n"
        f"<b>Город:</b> {city_name}\n"
        f"<b>Адрес:</b> {raw_address}\n"
        f"<b>Тип оборудования:</b> {service_request.equipment_type or ''}\n"
        "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
        f"<b>Комментарий:</b> {service_request.description or ''}"
    )

    # Текст кнопки
    amo_id = service_request.amo_crm_lead_id or service_request.id
    finish_button_text = f"Взять заявку {amo_id}"

    return {
        "message_for_masters": message_for_masters,
        "message_for_admin": message_for_admin,
        "finish_button_text": finish_button_text
    }


def handle_completed_deal(service_request, operator_comment, previous_status, lead_id, skip_commission=False):
    """
    Обработка сделки со статусом 'Completed':
     1) Считаем комиссию из ServiceType по имени
     2) Создаем транзакцию с типом Comission (если skip_commission=False)
     3) Отправляем POST на sambot
     4) Обрабатываем итог работы (WorkOutcome) — если в справочнике outcome есть штраф, списываем
     5) Пересчитываем уровень мастера
    """
    from decimal import Decimal
    import requests
    import logging
    from .models import ServiceType, WorkOutcome, Transaction

    logger = logging.getLogger(__name__)

    # 1) Сумма сделки
    deal_amount = service_request.price or Decimal('0.00')
    deal_amount = deal_amount - (service_request.spare_parts_spent or Decimal('0.00'))

    master_profile = service_request.master
    if not master_profile:
        logger.warning(f"ServiceRequest {service_request.id}: no master assigned, skipping commission")
        return

    # Текущий уровень мастера
    master_level = master_profile.level
    service_type_name = service_request.service_name
    service_type = ServiceType.objects.filter(name=service_type_name).first() if service_type_name else None

    if not service_type:
        logger.warning(
            "ServiceRequest %s: ServiceType '%s' not found, комиссия = 0",
            service_request.id, service_type_name
        )
        commission_percentage = Decimal('0.0')
    else:
        if master_level == 1:
            commission_percentage = service_type.commission_level_1 or Decimal('0.0')
        elif master_level == 2:
            commission_percentage = service_type.commission_level_2 or Decimal('0.0')
        elif master_level == 3:
            commission_percentage = service_type.commission_level_3 or Decimal('0.0')
        else:
            commission_percentage = Decimal('0.0')

    commission_amount = deal_amount * commission_percentage / Decimal('100')

    # Если не пропускаем комиссию
    if not skip_commission:
        Transaction.objects.create(
            master=master_profile,
            amount=commission_amount,
            transaction_type='Comission',
            status='Confirmed',
            service_request=service_request
        )

    # Шлём данные в sambot
    payload = {
        "request_id": lead_id,
        "telegram_id": master_profile.user.telegram_id,
        "penalty_message": "",
        "request_amount": deal_amount,
        "comission_amount": commission_amount,
        "previous_status": previous_status,
        "crm_operator_comment": operator_comment
    }
    try:
        response_sambot = requests.post(
            'https://sambot.ru/reactions/2939784/start?token=yhvtlmhlqbj',
            json=payload,
            timeout=10
        )
        if response_sambot.status_code != 200:
            logger.error(f"Failed to send data for Request {service_request.id}, status={response_sambot.status_code}")
    except Exception as ex:
        logger.error(f"Error sending data to sambot: {ex}")

    # 4) Обработка итогов работы (WorkOutcome).
    #    См. поле 'deal_success', ищем WorkOutcome из справочника
    if service_request.deal_success:
        outcome_record = WorkOutcome.objects.filter(outcome_name=service_request.deal_success).first()
        if outcome_record:
            # Если у справочного исхода is_penalty=True, делаем транзакцию Penalty
            if outcome_record.is_penalty:
                penalty_amount = outcome_record.penalty_amount or Decimal('0.0')
                Transaction.objects.create(
                    master=master_profile,
                    amount=penalty_amount,
                    transaction_type='Penalty',
                    status='Confirmed',
                    service_request=service_request
                )
                logger.info(f"Penalty {penalty_amount} recorded for master {master_profile.id}")

            # Привязываем (ForeignKey)
            service_request.work_outcome = outcome_record
            service_request.save()
        else:
            logger.warning(
                f"WorkOutcome with name '{service_request.deal_success}' not found for Request {service_request.id}"
            )

    # 5) Пересчитываем уровень
    recalc_master_level(master_profile)

def recalc_master_level(master_profile):
    """
    Пересчитывает уровень мастера на основе:
      1) Разницы между количеством успешных заявок (Completed с WorkOutcome, где is_success=True) и закрытых заявок.
      2) Количества приглашённых мастеров с подтверждённым депозитом.
      3) Условий перехода, которые хранятся в базе данных (в модели Settings).
      4) Признака вступления в группу (поле joined_group у мастера).
      
    Переменная difference – разница между числом успешных заявок и числом закрытых заявок,
    отражающая "чистый" показатель успешности работы мастера.
    """
    user = master_profile.user
    current_level = master_profile.level

    # Подсчитываем успешные заявки с WorkOutcome (где is_success=True)
    completed_count = ServiceRequest.objects.filter(
        master=master_profile,
        status='Completed',
        work_outcome__is_success=True
    ).count()
    # Подсчитываем закрытые заявки
    closed_count = ServiceRequest.objects.filter(
        master=master_profile,
        status='Closed'
    ).count()

    # Разница успешных и закрытых заявок – показатель чистой успешности
    difference = completed_count - closed_count

    # Количество приглашённых мастеров с подтверждённым депозитом
    invited_with_deposit = count_invited_masters_with_deposit(user)

    # Получаем условия перехода из настроек
    settings_obj = Settings.objects.first()
    if settings_obj:
        req_orders_level2 = settings_obj.required_orders_level2
        req_invites_level2 = settings_obj.required_invites_level2
        req_orders_level3 = settings_obj.required_orders_level3
        req_invites_level3 = settings_obj.required_invites_level3
    else:
        req_orders_level2, req_invites_level2 = 10, 1
        req_orders_level3, req_invites_level3 = 30, 3

    new_level = current_level  # по умолчанию оставляем текущий уровень

    # Для перехода на 3-й уровень мастер должен:
    # – иметь difference >= req_orders_level3,
    # – приглашено не менее req_invites_level3 мастеров,
    # – И, обязательно, иметь joined_group == True.
    if difference >= req_orders_level3 and invited_with_deposit >= req_invites_level3 and master_profile.joined_group:
        new_level = 3
    # Если условия для уровня 3 не выполнены, но для уровня 2 – переходим на 2-й уровень
    elif difference >= req_orders_level2 and invited_with_deposit >= req_invites_level2:
        new_level = 2
    else:
        new_level = 1

    # Проверка на понижение уровня (если показатели ухудшились)
    if current_level == 3:
        if difference < req_orders_level3 * 0.8 or invited_with_deposit < req_invites_level3:
            if difference >= req_orders_level2 and invited_with_deposit >= req_invites_level2:
                new_level = 2
            else:
                new_level = 1
    elif current_level == 2:
        if difference < req_orders_level2 * 0.8 or invited_with_deposit < req_invites_level2:
            new_level = 1

    if new_level != current_level:
        master_profile.level = new_level
        master_profile.save(update_fields=["level"])
        logger.info(f"Master {master_profile.id} level changed from {current_level} to {new_level}.")




def count_invited_masters_with_deposit(user: User) -> int:
    """
    Считает, сколько Мастеров (role='Master'), приглашённых данным user,
    имеют хотя бы один Confirmed Deposit.
    """
    # 1) Находим всех рефералов user с ролью 'Master'
    invited_masters = User.objects.filter(referrer=user, role='Master')

    # 2) Оставляем только тех, у кого есть хотя бы одна транзакция Deposit в статусе Confirmed
    invited_with_deposit = invited_masters.filter(
        transactions__transaction_type='Deposit',
        transactions__status='Confirmed'
    ).distinct()

    return invited_with_deposit.count()


class MasterStatisticsView(APIView):
    """
    API-эндпоинт для получения баланса и количества заявок мастера по telegram_id.
    """
    @swagger_auto_schema(
        operation_description="Получение баланса и количества заявок мастера по его telegram_id.",
        request_body=MasterStatisticsRequestSerializer,
        responses={
            200: openapi.Response(
                description="Баланс и количество заявок мастера",
                schema=MasterStatisticsResponseSerializer
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(type=openapi.TYPE_ARRAY, items=openapi.Items(type=openapi.TYPE_STRING))
                    }
                )
            ),
            404: openapi.Response(
                description="Мастер не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        # Валидируем входные данные
        serializer = MasterStatisticsRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        telegram_id = serializer.validated_data['telegram_id']

        # Пытаемся найти пользователя по telegram_id
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Пользователь с указанным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )

        # Проверяем, является ли пользователь мастером
        if not hasattr(user, 'master_profile'):
            return Response({"detail": "Пользователь не является мастером."}, status=status.HTTP_403_FORBIDDEN)

        master = user.master_profile

        # Получаем данные мастера
        balance = master.balance
        balance_sum = balance
        active_requests_count = ServiceRequest.objects.filter(master=master, status__in=['In Progress', 'AwaitingClosure', 'QualityControl']).count()

        # Проверка баланса: если он отрицательный, возвращаем 0, иначе 1
        balance = 0 if balance < 0 else 1

        # Формируем ответ
        return Response({
            "balance": balance,
            "active_requests_count": active_requests_count,
            "balance_sum": balance_sum
        }, status=status.HTTP_200_OK)
    

class FinishRequestView(APIView):
    """
    API-эндпоинт для завершения заявки мастером (или ботом).
    Переводит заявку в статус "Контроль качества", обновляет данные в AmoCRM,
    списывает комиссию (создаётся транзакция типа "Comission") и возвращает динамическое сообщение.
    """
    @swagger_auto_schema(
        operation_description="Закрытие заявки. Переводит заявку в статус 'Контроль качества', обновляет данные в AmoCRM и списывает комиссию.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'request_id': openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки"),
                'finalAnsw1': openapi.Schema(type=openapi.TYPE_STRING, description="Какие работы были выполнены"),
                'finalAnsw2': openapi.Schema(type=openapi.TYPE_STRING, description="Гарантия"),
                'finalAnsw3': openapi.Schema(type=openapi.TYPE_STRING, description="Итоговая цена (число)"),
                'finalAnsw4': openapi.Schema(type=openapi.TYPE_STRING, description="Сумма, потраченная на запчасти"),
                'finish_button_text': openapi.Schema(type=openapi.TYPE_STRING, description="Текст кнопки завершения с ID заявки")
            },
            required=['request_id']
        ),
        responses={
            200: openapi.Response(
                description="Заявка переведена в статус 'Контроль качества'",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING, description='Сообщение об успешном завершении'),
                        'client_telegram_id': openapi.Schema(type=openapi.TYPE_STRING),
                        'request_id': openapi.Schema(type=openapi.TYPE_STRING),
                        'message': openapi.Schema(type=openapi.TYPE_STRING, description="Динамическое сообщение для клиента"),
                        'has_client_review': openapi.Schema(type=openapi.TYPE_BOOLEAN, description="Флаг наличия отзыва клиента")
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Заявка не найдена",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            500: openapi.Response(
                description="Внутренняя ошибка сервера",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        data = request.data

        finalAnsw1 = data.get('finalAnsw1', "")      # Какие работы были выполнены
        finalAnsw2 = data.get('finalAnsw2', "")      # Гарантия
        finalAnsw3 = data.get('finalAnsw3', "")      # Итоговая цена (число)
        finalAnsw4 = data.get('finalAnsw4', "")      # Сумма, потраченная на запчасти
        finish_button_text = data.get('finish_button_text', "")  # Текст кнопки, содержащий ID заявки

        match = re.findall(r"\d+", finish_button_text)
        if not match:
            return JsonResponse({"error": "Не удалось извлечь ID из текста кнопки."}, status=400)

        request_id_str = match[0]
        try:
            request_id = int(request_id_str)
        except ValueError:
            return JsonResponse({"error": "Извлеченный ID не является числом."}, status=400)

        try:
            with transaction.atomic():
                service_request = ServiceRequest.objects.select_for_update().get(amo_crm_lead_id=request_id)

                # Проверяем, что заявка находится в статусе "In Progress" (В работе)
                if service_request.status != 'In Progress':
                    return JsonResponse({"detail": "Заявка должна быть в статусе 'В работе' для завершения."},
                                        status=400)

                price_value = Decimal(finalAnsw3) if finalAnsw3 else Decimal("0")
                spare_parts_value = Decimal(finalAnsw4) if finalAnsw4 else Decimal("0")

                service_request.comment_after_finish = finalAnsw1
                service_request.warranty = finalAnsw2
                service_request.price = price_value
                service_request.spare_parts_spent = spare_parts_value
                service_request.status = 'QualityControl'
                service_request.end_date = timezone.now()
                service_request.save()

                # Логика списания комиссии
                if service_request.master:
                    master_profile = service_request.master
                    master_level = master_profile.level
                    service_type_name = service_request.service_name
                    service_type = None
                    if service_type_name:
                        service_type = ServiceType.objects.filter(name=service_type_name).first()
                    if not service_type:
                        commission_percentage = Decimal('0.0')
                        logger.warning(
                            f"ServiceRequest {service_request.id}: ServiceType with name='{service_type_name}' not found. Commission = 0 by default."
                        )
                    else:
                        if master_level == 1:
                            commission_percentage = service_type.commission_level_1 or Decimal('0.0')
                        elif master_level == 2:
                            commission_percentage = service_type.commission_level_2 or Decimal('0.0')
                        elif master_level == 3:
                            commission_percentage = service_type.commission_level_3 or Decimal('0.0')
                        else:
                            commission_percentage = Decimal('0.0')
                    deal_amount = price_value - spare_parts_value
                    commission_amount = deal_amount * commission_percentage / Decimal('100')
                    Transaction.objects.create(
                        master=master_profile,
                        amount=commission_amount,
                        transaction_type='Comission',
                        status='Confirmed',
                        service_request=service_request
                    )
                    logger.info(f"Commission transaction created: {commission_amount} for master {master_profile.user.id}")

                    # Отправляем сообщение о списанной комиссии (округляем сумму вверх)
                    payload = {
                        "master_telegram_id": master_profile.user.telegram_id,
                        "message": f"С вас списана комиссия в размере {ceil(float(commission_amount))} монет по заявке {request_id}.\n\nВажно! Для того, чтобы получать новые заказы, необходимо иметь положительный баланс."
                    }
                    try:
                        response_msg = requests.post(
                            'https://sambot.ru/reactions/2849416/start?token=yhvtlmhlqbj',
                            json=payload,
                            timeout=10
                        )
                        if response_msg.status_code != 200:
                            logger.error(
                                f"Failed to send commission info to sambot. Status code: {response_msg.status_code}, Response: {response_msg.text}"
                            )
                    except Exception as ex:
                        logger.error(f"Error sending commission info to sambot: {ex}")

                lead_id = service_request.amo_crm_lead_id
                if not lead_id:
                    return JsonResponse({'error': 'AmoCRM lead_id is missing'}, status=400)

                amocrm_client = AmoCRMClient()
                custom_fields = [
                    {
                        "field_id": 735560,  # Сколько денег потрачено на запчасти
                        "values": [{"value": finalAnsw4}]
                    },
                    {
                        "field_id": 732020,  # Гарантия
                        "values": [{"value": finalAnsw2}]
                    },
                    {
                        "field_id": 743673,  # Какие работы были выполнены
                        "values": [{"value": finalAnsw1}]
                    }
                ]
                amocrm_client.update_lead(
                    lead_id,
                    {
                        "status_id": STATUS_MAPPING["QualityControl"],
                        "price": int(price_value),   # приводим к int
                        "custom_fields_values": custom_fields
                    }
                )

                # Формирование динамического сообщения для клиента
                device_type = service_request.equipment_type or "оборудование"
                brand = service_request.equipment_brand or "Bosch"
                master_name = master_profile.user.name if service_request.master else "мастер"
                master_rating = master_profile.rating if service_request.master else Decimal("0.0")
                rating_display = f"{int(master_rating)} ⭐"  # Выводим целое число рейтинга и рядом звезду
                cost = int(price_value)

                message_text = (
                    "🎉 Ремонт завершен!\n\n"
                    f"Ваш {device_type} марки {brand} успешно отремонтирован мастером {master_name}.\n\n"
                    "👨‍🔧 Выполненные работы:\n"
                    f"{finalAnsw1}\n\n"
                    f"💼 Рейтинг мастера: {rating_display}\n\n"
                    "💸 Стоимость работ:\n"
                    f"{cost} рублей.\n\n"
                    "Спасибо за доверие! Если возникнут вопросы или потребуется помощь, обращайтесь!"
                )

            return JsonResponse(
                {
                    "detail": f"Заявка {request_id} успешно переведена в статус 'Контроль качества'.",
                    "client_telegram_id": service_request.client.telegram_id,
                    "request_id": service_request.amo_crm_lead_id,
                    "message": message_text
                },
                status=200
            )

        except ServiceRequest.DoesNotExist:
            return JsonResponse(
                {"detail": f"Заявка с ID={request_id} не найдена."},
                status=404
            )
        except Exception as e:
            logger.error(f"Unexpected error in finish_request: {e}")
            return JsonResponse(
                {"detail": "Произошла ошибка при завершении заявки."},
                status=500
            )


class MasterFreeRequestsView(APIView):
    """
    API-эндпоинт для получения списка "свободных" заявок (status='Free'),
    соответствующих городу и типу оборудования мастера, 
    отсортированных по дате (ASC), максимум 10.
    """

    @swagger_auto_schema(
        operation_description="Получение свободных (статус 'Free') заявок, подходящих мастеру по городу и типу оборудования. "
                              "Сортировка по дате (ASC), максимум 10. Подстрочное совпадение для city_name и equipment_type.",
        request_body=MasterActiveRequestsSerializer,
        responses={
            200: openapi.Response(
                description="Успешный ответ со списком подходящих заявок. "
                            "Возвращает объект, где ключи request_1..request_N (до 10) содержат инфо о заявке.",
                # Используем additionalProperties, чтобыSwagger понимал, что может быть много ключей request_X
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    additionalProperties=openapi.Schema(
                        type=openapi.TYPE_OBJECT,
                        properties={
                            "message_text": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                description="Многострочный текст заявки с тегами <b> для жирного"
                            ),
                            "take_button_text": openapi.Schema(
                                type=openapi.TYPE_STRING,
                                description="Текст кнопки вида 'Взять заявку (ID)'"
                            )
                        }
                    )
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'field_name': openapi.Schema(
                            type=openapi.TYPE_ARRAY, 
                            items=openapi.Items(type=openapi.TYPE_STRING)
                        )
                    }
                )
            ),
            403: openapi.Response(
                description="Доступ запрещен",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Мастер не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
            # Если нужно, можно добавить описание для 500.
        }
    )
    def post(self, request):
        serializer = MasterActiveRequestsSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        telegram_id = serializer.validated_data['telegram_id']

        # 1) Проверяем пользователя
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response({"detail": "Пользователь с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)

        if user.role != 'Master':
            return Response({"detail": "Доступно только для пользователей с ролью 'Master'."},
                            status=status.HTTP_403_FORBIDDEN)

        # 2) Получаем master_profile
        try:
            master = user.master_profile  # или user.master
        except AttributeError:
            return Response({"detail": "Мастер не найден для данного пользователя."},
                            status=status.HTTP_404_NOT_FOUND)

        # Предполагаем, что у мастера в полях city_name / equipment_type_name может быть перечислено через запятую
        master_cities_str = (master.city_name or "").lower()          
        master_equip_str = (master.equipment_type_name or "").lower() 

        # 3) Собираем заявки Free, сортируем по created_at (ASC)
        free_requests = ServiceRequest.objects.filter(status='Free').order_by('created_at')

        # 4) Фильтруем (если req.city_name и req.equipment_type входят в master'ские строки)
        matched_requests = []
        for req in free_requests:
            req_city = (req.city_name or "").lower()
            req_equip = (req.equipment_type or "").lower()
            if req_city in master_cities_str and req_equip in master_equip_str:
                matched_requests.append(req)

        # Берём первые 10
        matched_requests = matched_requests[:10]

        # Если нет подходящих заявок
        if not matched_requests:
            return Response(
                {
                    "request_1": {
                        "message_text": "🥳Нет свободных заявок!",
                        "take_button_text": ""
                    }
                },
                status=status.HTTP_200_OK
            )

        # Формируем ответ { "request_1": {...}, "request_2": {...}, ... }
        result = {}
        for i, req in enumerate(matched_requests):
            field_name = f"request_{i+1}"

            # Форматированная дата
            date_str = req.created_at.strftime('%d.%m.%Y') if req.created_at else ""

            # Берём только первое слово адреса
            raw_address = (req.address or "").strip()
            address_parts = raw_address.split()
            short_address = address_parts[0] if address_parts else ""

            # Формируем текст по образцу:
            message_text = (
                f"<b>Заявка </b> {req.amo_crm_lead_id}\n"
                f"<b>Дата заявки:</b> {date_str} г.\n"
                f"<b>Город:</b> {req.city_name or ''}\n"
                f"<b>Адрес: </b> {short_address}\n"
                f"<b>Тип оборудования:</b> {req.equipment_type or ''}\n"
                f"<b>Модель:</b> {req.equipment_brand or '-'}\n"
                f"<b>Марка:</b> {req.equipment_model or '-'}\n"
                "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                f"<b>Комментарий:</b> {req.description or ''}\n"
                "🔸🔸🔸🔸🔸🔸🔸🔸🔸🔸\n"
                "<b>Бесплатный выезд и диагностика*</b> - Бесплатный выезд и диагностика "
                "только при оказание ремонта. ВНИМАНИЕ! - В случае отказа от ремонта - "
                "Диагностика и выезд платные берется с клиента (Цену формирует мастер)"
            )

            take_button_text = f"Взять заявку {req.amo_crm_lead_id}"

            result[field_name] = {
                "message_text": message_text,
                "take_button_text": take_button_text
            }

        return Response(result, status=status.HTTP_200_OK)



class ClientRequestsView(APIView):
    """
    API-эндпоинт для получения заявок клиента, сгруппированных по статусам.
    """

    @swagger_auto_schema(
        operation_description="Получение заявок клиента, сгруппированных по категориям: "
                              "На проверке, В работе, Поиск мастера, Завершено.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'telegram_id': openapi.Schema(type=openapi.TYPE_STRING, description="Telegram ID клиента")
            },
            required=['telegram_id']
        ),
        responses={
            200: openapi.Response(
                description="Сформированный текст с группировкой заявок",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "text": openapi.Schema(type=openapi.TYPE_STRING, description="HTML-текст с заявками")
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Клиент не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        # Проверка и валидация входных данных
        telegram_id = request.data.get('telegram_id')
        if not telegram_id:
            return Response({"detail": "telegram_id обязателен."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = User.objects.get(telegram_id=telegram_id, role='Client')
        except User.DoesNotExist:
            return Response({"detail": "Клиент с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)

        # Получаем все заявки клиента
        client_requests = ServiceRequest.objects.filter(client=user).order_by('created_at')

        # Группируем заявки по категориям
        groups = {
            "На проверке": [],
            "В работе": [],
            "Поиск мастера": [],
            "Завершено": []
        }

        for req in client_requests:
            status_value = req.status
            if status_value == 'Open':
                groups["На проверке"].append(req)
            elif status_value == 'In Progress':
                groups["В работе"].append(req)
            elif status_value == 'Free':
                groups["Поиск мастера"].append(req)
            elif status_value in ['Completed', 'AwaitingClosure', 'Closed', 'QualityControl']:
                groups["Завершено"].append(req)

        # Формирование текста ответа
        output_lines = []
        for category in ["На проверке", "В работе", "Поиск мастера", "Завершено"]:
            if groups[category]:
                output_lines.append(f"<b>{category}</b>")
                for req in groups[category]:
                    name = req.equipment_type or ""
                    output_lines.append(f"Заказ {req.amo_crm_lead_id}: {name}")
                output_lines.append("")  # добавляем пустую строку для разделения групп

        # Если нет ни одной заявки во всех категориях
        if not output_lines:
            output_lines.append("🥳Нет заявок!")

        final_text = "\n".join(output_lines)
        buttons = [(req.amo_crm_lead_id) for req in client_requests if req.amo_crm_lead_id]

        return Response({"requests": final_text, "buttons": buttons}, status=status.HTTP_200_OK)

class ClientRequestInfoView(APIView):
    """
    API-эндпоинт для получения детальной информации о заявке клиента.
    """

    @swagger_auto_schema(
        operation_description=(
            "Получение детальной информации о заявке по request_id. "
            "Если мастер не назначен или заявка не завершена, соответствующие поля остаются пустыми."
        ),
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'request_id': openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки")
            },
            required=['request_id']
        ),
        responses={
            200: openapi.Response(
                description="Информация о заявке клиента",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "text": openapi.Schema(type=openapi.TYPE_STRING, description="Детальная информация о заявке")
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Заявка не найдена",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        request_id = request.data.get('request_id')

        if not request_id:
            return Response({"detail": "request_id обязателен."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Пытаемся получить заявку по amo_crm_lead_id, предполагая, что request_id совпадает с amo_crm_lead_id
            req = ServiceRequest.objects.get(amo_crm_lead_id=request_id)
        except ServiceRequest.DoesNotExist:
            return Response({"detail": "Заявка не найдена."}, status=status.HTTP_404_NOT_FOUND)

        # Получаем информацию из заявки
        client_name = req.client.name if req.client else ""
        order_id = req.amo_crm_lead_id
        equipment = req.equipment_type or ""
        date_created = req.created_at.strftime('%d.%m.%Y') if req.created_at else ""
        status_display = req.get_status_display() if hasattr(req, 'get_status_display') else req.status

        finished_statuses = ['Completed', 'AwaitingClosure', 'Closed', 'QualityControl']
        if req.master and req.status in finished_statuses:
            master_name = f"{req.master.user.name}" if req.master.user else ""
            start_date = ""  # Нет данных о дате начала работ
            end_date = ""    # Нет данных о дате окончания работ
            warranty = req.warranty or ""
            if req.price is not None:
                # Округление стоимости до целого числа без дробной части
                rounded_price = req.price.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
                cost = str(rounded_price)
            else:
                cost = ""
            comment_after_finish = req.comment_after_finish or ""
        else:
            master_name = ""
            start_date = ""
            end_date = ""
            warranty = ""
            cost = ""
            comment_after_finish = ""

        response_text = (
            f"<b>Заказ</b>: {order_id}\n"
            f"{equipment}\n"
            f"<b>Дата заявки:</b> {date_created} г.\n"
            "---------\n"
            f"<b>Статус:</b> {status_display}\n"
            f"<b>Мастер:</b> {master_name}\n"
            f"<b>Дата начала работ:</b> {start_date}\n"
            f"<b>Дата окончания работ:</b> {end_date}\n"
            "----------\n"
            f"<b>Гарантия:</b> {warranty}\n"
            f"<b>Стоимость заказа:</b> {cost}\n"
            "----------\n"
            f"<b>Проделанные работы:</b> {comment_after_finish}"
        )

        return Response({"text": response_text}, status=status.HTTP_200_OK)


class MasterStatsView(APIView):
    """
    Возвращает JSON c рассчитанными полями статистики мастера (по telegram_id)
    и текстовым представлением реального ТОП-10 мастеров, отсортированного по суммарному доходу.
    """
    @swagger_auto_schema(
        operation_description="POST-запрос, возвращает статистику мастера и реальный ТОП-10 мастеров (строками).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'telegram_id': openapi.Schema(
                    type=openapi.TYPE_STRING, 
                    description="Telegram ID мастера"
                )
            },
            required=['telegram_id']
        ),
        responses={
            200: openapi.Response(
                description="Успешный ответ со статистикой мастера и ТОП-10 мастеров",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "fio": openapi.Schema(type=openapi.TYPE_STRING),
                        "registration_date": openapi.Schema(type=openapi.TYPE_STRING),
                        "rating": openapi.Schema(type=openapi.TYPE_STRING),
                        "completed_orders": openapi.Schema(type=openapi.TYPE_INTEGER),
                        "avg_time": openapi.Schema(type=openapi.TYPE_STRING),
                        "total_income": openapi.Schema(type=openapi.TYPE_STRING),
                        "quality_percent": openapi.Schema(type=openapi.TYPE_STRING),
                        "balance_topup_speed": openapi.Schema(type=openapi.TYPE_STRING),
                        "cost_percentage": openapi.Schema(type=openapi.TYPE_STRING),
                        "current_status": openapi.Schema(type=openapi.TYPE_STRING, description="Номер круга, в который подходит мастер"),
                        "rating_place": openapi.Schema(type=openapi.TYPE_STRING),
                        "top_10": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Многострочный текст с ТОП-10 мастеров"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Мастер не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={'detail': openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get('telegram_id')

        if not telegram_id:
            return Response(
                {"detail": "Поле telegram_id обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response({"detail": "Мастер с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)

        master = getattr(user, 'master_profile', None)
        if not master:
            return Response({"detail": "Пользователь не является мастером."},
                            status=status.HTTP_404_NOT_FOUND)

        # -----------------------------------
        # Вычисляем статистику мастера
        # -----------------------------------
        finished_statuses = ['Completed', 'AwaitingClosure', 'Closed', 'QualityControl']
        completed_qs = ServiceRequest.objects.filter(master=master, status__in=finished_statuses)

        completed_orders_count = completed_qs.count()
        total_income_value = completed_qs.aggregate(sum_price=Sum('price'))['sum_price'] or Decimal("0")
        master_rating = master.rating or Decimal("0.0")

        avg_time_seconds = 0
        count_for_avg = 0
        for req in completed_qs:
            if req.start_date and req.end_date:
                delta = req.end_date - req.start_date
                avg_time_seconds += delta.total_seconds()
                count_for_avg += 1
        avg_hours = int((avg_time_seconds / count_for_avg) // 3600) if count_for_avg > 0 else 0
        avg_time_str = f"{avg_hours} часов"

        # Рассчитываем качество работ как процент от 5 баллов
        qs_quality = ServiceRequest.objects.filter(master=master, quality_rating__isnull=False)
        if qs_quality.exists():
            avg_quality = qs_quality.aggregate(avg=Avg('quality_rating'))['avg']
            quality_percent = round((avg_quality / 5) * 100)
        else:
            quality_percent = 0
        quality_percent_str = f"{quality_percent}%"

        # Скорость пополнения баланса
        deposit_qs = master.user.transactions.filter(transaction_type='Deposit', status='Confirmed').order_by('created_at')
        if deposit_qs.count() >= 2:
            time_diffs = []
            deposits = list(deposit_qs)
            for i in range(1, len(deposits)):
                diff = (deposits[i].created_at - deposits[i-1].created_at).total_seconds()
                time_diffs.append(diff)
            avg_diff_hours = int(sum(time_diffs) / len(time_diffs) // 3600)
            balance_topup_speed_str = f"{avg_diff_hours} часов"
        else:
            balance_topup_speed_str = "Нет данных"

        # Процент затрат на запчасти
        total_cost = completed_qs.aggregate(total_cost=Sum('spare_parts_spent'))['total_cost'] or Decimal("0")
        if total_income_value > 0:
            cost_percentage = round((total_cost / total_income_value) * 100)
        else:
            cost_percentage = 0
        cost_percentage_str = f"{cost_percentage}%"

        # Текущий статус как номер круга, в который подходит мастер.
        # Для этого получаем статистику мастера (success_ratio, cost_ratio, last_deposit)
        success_ratio, cost_ratio, last_deposit = get_master_statistics(master)
        if success_ratio >= 0.8 and cost_ratio <= 0.3 and last_deposit >= now() - timedelta(hours=24):
            current_round = "1-й круг"
        elif success_ratio >= 0.8 and 0.3 < cost_ratio <= 0.5:
            current_round = "2-й круг"
        else:
            current_round = "3-й круг"

        registration_date = user.created_at.strftime("%d.%m.%Y") if user.created_at else "—"

        data_for_master = {
            "fio": user.name,
            "registration_date": registration_date,
            "rating": f"{master_rating}⭐️",
            "completed_orders": completed_orders_count,
            "avg_time": avg_time_str,
            "total_income": f"{int(total_income_value)} руб.",
            "quality_percent": quality_percent_str,
            "balance_topup_speed": balance_topup_speed_str,
            "cost_percentage": cost_percentage_str,
            "current_status": current_round,
            "rating_place": "—",  # будет обновлено ниже
        }

        # Реальный ТОП-10 мастеров (доход по завершённым заявкам)
        all_masters = Master.objects.all()
        stats_list = []
        for m in all_masters:
            m_finished_qs = ServiceRequest.objects.filter(master=m, status__in=finished_statuses)
            m_income = m_finished_qs.aggregate(sum_price=Sum('price'))['sum_price'] or Decimal("0")
            m_rating = m.rating or Decimal("0.0")
            m_cities = m.city_name or ""
            stats_list.append((m, m_income, m_rating, m_cities))
        stats_list.sort(key=lambda x: x[1], reverse=True)

        for idx, item in enumerate(stats_list, start=1):
            if item[0].id == master.id:
                data_for_master["rating_place"] = f"{idx} место"
                break

        top_10_data = stats_list[:10]
        lines = []
        for idx, (m, inc, rat, cts) in enumerate(top_10_data, start=1):
            line = f"{idx}.| {m.user.name}| {cts}| {int(inc)} руб.| {rat}⭐️"
            lines.append(line)
        top_10_str = "\n\n".join(lines)

        result = {**data_for_master, "top_10": top_10_str}
        return Response(result, status=status.HTTP_200_OK)



class BalanceDepositView(APIView):
    """
    1. Создаёт транзакцию со статусом 'Pending' для пополнения баланса.
    2. Не изменяет баланс мастера сразу.
    3. Возвращает ID созданной транзакции (transaction_id).
    """

    @swagger_auto_schema(
        operation_description="Создаёт транзакцию (статус='Pending') для пополнения баланса мастера. Возвращает ID транзакции.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'telegram_id': openapi.Schema(
                    type=openapi.TYPE_STRING, 
                    description="Telegram ID мастера"
                ),
                'amount': openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Сумма для пополнения в виде строки (например '100.50')"
                )
            },
            required=['telegram_id', 'amount']
        ),
        responses={
            200: openapi.Response(
                description="Транзакция создана (статус 'Pending').",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING),
                        "transaction_id": openapi.Schema(type=openapi.TYPE_INTEGER)
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные (telegram_id или amount не указаны / неверный формат)",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Мастер не найден / пользователь не является мастером",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get('telegram_id')
        amount_str = data.get('amount')

        if not telegram_id or not amount_str:
            return Response(
                {"detail": "Поля 'telegram_id' и 'amount' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Пробуем конвертировать amount в Decimal
        try:
            amount = Decimal(amount_str)
            if amount <= 0:
                return Response(
                    {"detail": "Сумма пополнения должна быть больше нуля."},
                    status=status.HTTP_400_BAD_REQUEST
                )
        except:
            return Response(
                {"detail": "Некорректный формат суммы."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Ищем пользователя
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Пользователь с указанным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )

        # Проверяем, является ли пользователь мастером
        master = getattr(user, 'master_profile', None)
        if not master:
            return Response(
                {"detail": "Пользователь не является мастером."},
                status=status.HTTP_404_NOT_FOUND
            )

        # Создаём транзакцию со статусом 'Pending'
        with transaction.atomic():
            new_tx = Transaction.objects.create(
                master = master,
                amount=amount,
                transaction_type='Deposit',
                status='Pending',  # можно не указывать, если в модели стоит default='Pending'
                reason="Пополнение (ожидает подтверждения)"
            )

        return Response(
            {
                "detail": "Транзакция на пополнение создана (статус 'Pending').",
                "transaction_id": new_tx.id
            },
            status=status.HTTP_200_OK
        )

class BalanceDepositConfirmView(APIView):
    """
    Подтверждает транзакцию пополнения (по transaction_id),
    переводит её в статус 'Confirmed' и увеличивает баланс мастера.
    Также начисляет бонусы реферальной системы, но только при первом пополнении.
    """

    @swagger_auto_schema(
        operation_description="Подтверждает транзакцию пополнения (transaction_id), переводит её в статус 'Confirmed', "
                              "увеличивает баланс мастера и начисляет реферальные бонусы (ТОЛЬКО ПРИ ПЕРВОМ ПОПОЛНЕНИИ).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'transaction_id': openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    description="ID транзакции на пополнение (статус 'Pending')"
                )
            },
            required=['transaction_id']
        ),
        responses={
            200: openapi.Response(
                description="Транзакция подтверждена, баланс мастера обновлён, бонусы начислены (если первое пополнение).",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING),
                        "new_balance": openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные или транзакция уже подтверждена.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Транзакция или мастер не найдены.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        tx_id = data.get('transaction_id')

        if not tx_id:
            return Response({"detail": "Поле 'transaction_id' обязательно."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            tx_id = int(tx_id)
        except ValueError:
            return Response({"detail": "transaction_id должно быть целым числом."}, status=status.HTTP_400_BAD_REQUEST)

        with transaction.atomic():
            try:
                tx = Transaction.objects.select_for_update().get(id=tx_id)
            except Transaction.DoesNotExist:
                return Response({"detail": "Транзакция не найдена."}, status=status.HTTP_404_NOT_FOUND)
        
            if tx.status == 'Confirmed':
                return Response({"detail": "Транзакция уже подтверждена."}, status=status.HTTP_400_BAD_REQUEST)
        
            if tx.transaction_type != 'Deposit':
                return Response({"detail": "Транзакция не является пополнением (Deposit)."}, status=status.HTTP_400_BAD_REQUEST)
        
            # Подтверждаем транзакцию
            tx.status = 'Confirmed'
            tx.save()
        
            # Получаем мастера из транзакции
            master = tx.master
            if not master:
                return Response({"detail": "Мастер не найден."}, status=status.HTTP_404_NOT_FOUND)
        
            # Получаем пользователя из мастера
            user = master.user
            if not user:
                return Response({"detail": "Пользователь не найден."}, status=status.HTTP_404_NOT_FOUND)
        
            # Обновляем баланс мастера (например, увеличиваем баланс на сумму транзакции)
            master.balance += tx.amount
            master.save()
        
            # Проверяем, является ли это первое пополнение
            first_deposit = not Transaction.objects.filter(
                master=master,
                transaction_type='Deposit',
                status='Confirmed'
            ).exclude(id=tx.id).exists()
        
            # Если это первое пополнение, начисляем бонусы реферальной системы
            if first_deposit:
                ref_1 = user.referrer  # первая линия рефералов
                if ref_1 and ref_1.role == 'Master':
                    ref_1.master_profile.balance += Decimal(500)
                    ref_1.master_profile.save()
        
                    # Проверяем вторую линию
                    ref_2 = ref_1.referrer
                    if ref_2 and ref_2.role == 'Master':
                        ref_2.master_profile.balance += Decimal(250)
                        ref_2.master_profile.save()
        
            return Response({
                "detail": "Транзакция подтверждена, баланс мастера обновлён. " +
                          ("Бонусы начислены." if first_deposit else "Бонусы НЕ начислены (не первое пополнение)."),
                "new_balance": str(master.balance),
                "telegram_id": user.telegram_id
            }, status=status.HTTP_200_OK)


class DeactivateUserView(APIView):
    """
    Деактивирует пользователя (is_active = False) по указанному telegram_id.
    """

    @swagger_auto_schema(
        operation_description="Деактивировать пользователя по telegram_id",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'telegram_id': openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID пользователя, которого нужно деактивировать"
                )
            },
            required=['telegram_id']
        ),
        responses={
            200: openapi.Response(
                description="Пользователь успешно деактивирован",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING),
                    }
                )
            ),
            400: openapi.Response(
                description="Неправильные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Пользователь не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get('telegram_id')

        if not telegram_id:
            return Response(
                {"detail": "Поле 'telegram_id' обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )

        from .models import User  # Или где у вас лежит модель

        try:
            user = User.objects.get(telegram_id=telegram_id)
        except User.DoesNotExist:
            return Response(
                {"detail": "Пользователь с таким telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )

        # Деактивируем
        user.is_active = False
        user.save()

        return Response(
            {"detail": f"Пользователь {user.name} (telegram_id={telegram_id}) деактивирован."},
            status=status.HTTP_200_OK
        )

class ActivateUserView(APIView):
    """
    Активирует пользователя (is_active = True) по указанному telegram_id.
    """

    @swagger_auto_schema(
        operation_description="Активировать пользователя по telegram_id",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'telegram_id': openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID пользователя, которого нужно активировать"
                )
            },
            required=['telegram_id']
        ),
        responses={
            200: openapi.Response(
                description="Пользователь успешно активирован",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING),
                    }
                )
            ),
            400: openapi.Response(
                description="Неправильные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Пользователь не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get('telegram_id')

        if not telegram_id:
            return Response(
                {"detail": "Поле 'telegram_id' обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )

        from .models import User  # Импорт модели User

        try:
            user = User.objects.get(telegram_id=telegram_id)
        except User.DoesNotExist:
            return Response(
                {"detail": "Пользователь с таким telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )

        # Активируем пользователя
        user.is_active = True
        user.save()

        return Response(
            {"detail": f"Пользователь {user.name} (telegram_id={telegram_id}) активирован."},
            status=status.HTTP_200_OK
        )
class MasterProfileView(APIView):
    """
    API‑точка для отправки данных профиля мастера.
    Во входных данных ожидается telegram_id (у пользователя role="Master").
    Возвращает форматированное сообщение и доп.данные о мастере.
    """

    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        if not telegram_id:
            return Response(
                {"detail": "Поле 'telegram_id' обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # 1) Проверяем, что такой пользователь существует и является мастером
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Мастер с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )

        # 2) Получаем профиль мастера
        try:
            master = user.master_profile
        except Master.DoesNotExist:
            return Response(
                {"detail": "Профиль мастера не найден."},
                status=status.HTTP_404_NOT_FOUND
            )

        # Подсчитываем кол-во отзывов
        reviews_count = RatingLog.objects.filter(master=master).count()

        # 3) Считываем настройки перехода уровней из БД
        settings_obj = Settings.objects.first()
        if not settings_obj:
            # fallback, если настроек нет
            max_req_l1, max_req_l2, max_req_l3 = 1, 3, 5
            req_orders_level2, req_invites_level2 = 10, 1
            req_orders_level3, req_invites_level3 = 30, 3
        else:
            max_req_l1 = settings_obj.max_requests_level1
            max_req_l2 = settings_obj.max_requests_level2
            max_req_l3 = settings_obj.max_requests_level3
            req_orders_level2 = settings_obj.required_orders_level2
            req_invites_level2 = settings_obj.required_invites_level2
            req_orders_level3 = settings_obj.required_orders_level3
            req_invites_level3 = settings_obj.required_invites_level3

        current_level = master.level if master.level in (1, 2, 3) else 3

        # 4) Определяем комиссию для текущего/следующего уровня, исходя из service_name
        service_type_name = master.service_name or ""
        service_type = ServiceType.objects.filter(name=service_type_name).first()

        def safe_percent(val: Decimal|None) -> str:
            """Округляем и возвращаем строку вида '30%', либо '–', если None."""
            if val is None:
                return "–"
            return f"{int(val)}%"

        if not service_type:
            # Если такой service_type не найден => все комиссии 0
            commission_l1 = commission_l2 = commission_l3 = Decimal(0)
        else:
            commission_l1 = service_type.commission_level_1 or Decimal(0)
            commission_l2 = service_type.commission_level_2 or Decimal(0)
            commission_l3 = service_type.commission_level_3 or Decimal(0)

        if current_level == 1:
            cur_comm = commission_l1
            cur_max_req = max_req_l1
            next_comm = commission_l2
            next_max_req = max_req_l2
            req_works_for_next = req_orders_level2
            req_invites_for_next = req_invites_level2
            next_level = 2
        elif current_level == 2:
            cur_comm = commission_l2
            cur_max_req = max_req_l2
            next_comm = commission_l3
            next_max_req = max_req_l3
            req_works_for_next = req_orders_level3
            req_invites_for_next = req_invites_level3
            next_level = 3
        else:
            # Третий уровень — выше нет
            cur_comm = commission_l3
            cur_max_req = max_req_l3
            next_comm = None
            next_max_req = None
            req_works_for_next = 0
            req_invites_for_next = 0
            next_level = None

        # 5) Подсчитываем «успешные» заказы (WorkOutcome с is_success=True)
        completed_orders = ServiceRequest.objects.filter(
            master=master,
            status='Completed',
            work_outcome__is_success=True
        ).count()

        # 6) Подсчёт приглашённых мастеров (если нужно с депозитом — адаптируйте)
        invited_count = User.objects.filter(referrer=user, role="Master").count()

        # 7) Сколько осталось до следующего уровня
        remaining_works = 0
        remaining_invites = 0
        progress_works = 0
        progress_invites = 0

        if current_level < 3:
            # Работы
            need_works = req_works_for_next - completed_orders
            remaining_works = max(0, need_works)
            # Приглашения
            need_invites = req_invites_for_next - invited_count
            remaining_invites = max(0, need_invites)

            # Считаем проценты прогресса (от 0 до 100)
            if req_works_for_next > 0:
                progress_works = min(100, int((completed_orders / req_works_for_next) * 100))
            else:
                progress_works = 100
            if req_invites_for_next > 0:
                progress_invites = min(100, int((invited_count / req_invites_for_next) * 100))
            else:
                progress_invites = 100

        # итого берём минимум, чтобы для достижения 100% нужно было выполнить оба условия
        overall_progress = int((progress_works + progress_invites) / 2)

        # 8) Формируем наименование уровня через MASTER_LEVEL_MAPPING
        #    Предположим, в utils.py у вас есть словарь:
        #    MASTER_LEVEL_MAPPING = {1: "Мастер", 2: "Грандмастер", 3: "Учитель"}
        level_name = MASTER_LEVEL_MAPPING.get(current_level)

        # 9) Формируем итоговое сообщение
        message = (
            f"📋 <b>Мой профиль</b>\n"
            f"✏️ Имя: {user.name or ''}\n"
            f"📞 Телефон: {user.phone or ''}\n"
            f"🏙 Город: {master.city_name or ''}\n"
            f"⭐️ Рейтинг: {master.rating}\n"
            f"💬 Отзывы: {reviews_count}\n\n"
            f"🎖 Уровень: {level_name}\n\n"
            f"🚀 Прогресс по работам: {progress_works}%\n"
            f"🚀 Прогресс по приглашениям: {progress_invites}%\n"
            f"🏁 Итоговый прогресс: {overall_progress}%\n\n"
            f"<b>Награды и привилегии на вашем уровне:</b>\n"
            f"💸 Текущая комиссия: {safe_percent(cur_comm)}\n"
            f"🔨 Можно брать {cur_max_req} заявок\n\n"
        )

        if current_level < 3:
            next_level_name = MASTER_LEVEL_MAPPING.get(next_level, f"Уровень {next_level}")
            message += (
                f"<b>Что вас ждёт на следующем уровне:</b>\n"
                f"💸 Уменьшение комиссии: {safe_percent(next_comm)}\n"
                f"🔨 Можно брать {next_max_req} заявок\n\n"
                f"📈 <b>Развитие</b>:\n"
                f"🛠 Осталось выполнить работ: {remaining_works}\n"
                f"👤 Осталось пригласить мастеров: {remaining_invites}\n\n"
                f"🛠 <b>Виды работ:</b> {master.equipment_type_name}\n"
                f"🛠 <b>Вид услуг:</b> {service_type_name}\n"
            )
        else:
            message += "Вы уже на максимальном уровне!\n"

        response_data = {
            "message": message,
            "level": current_level,
            "city": master.city_name,
            "name": user.name,
            "equipment": master.equipment_type_name,
            "phone": user.phone,
        }
        return Response(response_data, status=status.HTTP_200_OK)

class MasterCityUpdateView(APIView):
    """
    API‑точка для обновления города мастера.
    Во входных данных ожидаются поля:
      - telegram_id: Telegram ID мастера
      - name: новое название города мастера
    """
    @swagger_auto_schema(
        operation_description="Обновляет город мастера. Принимает telegram_id и name (новый город).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID мастера"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новое название города мастера"
                )
            },
            required=["telegram_id", "name"]
        ),
        responses={
            200: openapi.Response(
                description="Город мастера обновлён",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Мастер или профиль не найдены",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_city = request.data.get("name")
        if not telegram_id or not new_city:
            return Response(
                {"detail": "Поля 'telegram_id' и 'name' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            # Ищем пользователя с ролью "Master"
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Мастер с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        try:
            master = user.master_profile
        except Master.DoesNotExist:
            return Response(
                {"detail": "Профиль мастера не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем город мастера
        master.city_name = new_city
        master.save()
        # При необходимости обновляем и город в модели User
        user.city_name = new_city
        user.save()
        return Response(
            {"detail": f"Город мастера обновлён на '{new_city}'."},
            status=status.HTTP_200_OK
        )

class MasterEquipmentUpdateView(APIView):
    """
    API‑точка для обновления строки списка оборудований мастера.
    Во входных данных ожидаются поля:
      - telegram_id: Telegram ID мастера
      - name: новая строка списка оборудований мастера
    """
    @swagger_auto_schema(
        operation_description="Обновляет строку списка оборудований мастера. Принимает telegram_id и name (новая строка оборудования).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID мастера"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новая строка списка оборудований мастера"
                )
            },
            required=["telegram_id", "name"]
        ),
        responses={
            200: openapi.Response(
                description="Строка списка оборудований мастера обновлена",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Мастер или профиль не найдены",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_equipment = request.data.get("name")
        if not telegram_id or not new_equipment:
            return Response(
                {"detail": "Поля 'telegram_id' и 'name' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Мастер с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        try:
            master = user.master_profile
        except Master.DoesNotExist:
            return Response(
                {"detail": "Профиль мастера не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем строку списка оборудований мастера
        master.equipment_type_name = new_equipment
        master.save()
        return Response(
            {"detail": f"Список оборудований мастера обновлён на '{new_equipment}'."},
            status=status.HTTP_200_OK
        )
    


class MasterPhoneUpdateView(APIView):
    """
    API‑точка для обновления номера телефона мастера.
    Во входных данных ожидаются:
      - telegram_id: Telegram ID мастера
      - name: новый номер телефона мастера
    """
    @swagger_auto_schema(
        operation_description="Обновляет номер телефона мастера. Принимает telegram_id и name (новый номер).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID мастера"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новый номер телефона мастера"
                )
            },
            required=["telegram_id", "name"]
        ),
        responses={
            200: openapi.Response(
                description="Номер телефона мастера обновлён",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Мастер с данным telegram_id не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_phone = request.data.get("name")
        if not telegram_id or not new_phone:
            return Response(
                {"detail": "Поля 'telegram_id' и 'phone' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            # Ищем именно мастера по telegram_id
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Мастер с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем номер телефона мастера
        user.phone = new_phone
        user.save()
        return Response(
            {"detail": f"Номер телефона мастера обновлён на '{new_phone}'."},
            status=status.HTTP_200_OK
        )


class MasterNameUpdateView(APIView):
    """
    API‑точка для обновления имени мастера.
    Во входных данных ожидаются:
      - telegram_id: Telegram ID мастера
      - name: новое имя мастера
    """
    @swagger_auto_schema(
        operation_description="Обновляет имя мастера. Принимает telegram_id и name (новое имя).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID мастера"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новое имя мастера"
                )
            },
            required=["telegram_id", "name"]
        ),
        responses={
            200: openapi.Response(
                description="Имя мастера обновлено",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Мастер с данным telegram_id не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_name = request.data.get("name")
        if not telegram_id or not new_name:
            return Response(
                {"detail": "Поля 'telegram_id' и 'name' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            # Ищем мастера по telegram_id
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Мастер с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем имя мастера
        user.name = new_name
        user.save()
        return Response(
            {"detail": f"Имя мастера обновлено на '{new_name}'."},
            status=status.HTTP_200_OK
        )
class ClientPhoneUpdateView(APIView):
    """
    API‑точка для обновления номера телефона клиента.
    Во входных данных ожидаются:
      - telegram_id: Telegram ID клиента
      - phone: новый номер телефона клиента
    """
    @swagger_auto_schema(
        operation_description="Обновляет номер телефона клиента. Принимает telegram_id и phone (новый номер).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID клиента"
                ),
                "phone": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новый номер телефона клиента"
                )
            },
            required=["telegram_id", "phone"]
        ),
        responses={
            200: openapi.Response(
                description="Номер телефона клиента обновлён",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Клиент с данным telegram_id не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_phone = request.data.get("phone")
        if not telegram_id or not new_phone:
            return Response(
                {"detail": "Поля 'telegram_id' и 'phone' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            # Ищем клиента по telegram_id и роли "Client"
            user = User.objects.get(telegram_id=telegram_id, role="Client")
        except User.DoesNotExist:
            return Response(
                {"detail": "Клиент с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем номер телефона клиента
        user.phone = new_phone
        user.save()
        return Response(
            {"detail": f"Номер телефона клиента обновлён на '{new_phone}'."},
            status=status.HTTP_200_OK
        )


class ClientCityUpdateView(APIView):
    """
    API‑точка для обновления города клиента.
    Во входных данных ожидаются:
      - telegram_id: Telegram ID клиента
      - name: новое название города клиента
    """
    @swagger_auto_schema(
        operation_description="Обновляет город клиента. Принимает telegram_id и name (новый город).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID клиента"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новое название города клиента"
                )
            },
            required=["telegram_id", "name"]
        ),
        responses={
            200: openapi.Response(
                description="Город клиента обновлён",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Клиент с данным telegram_id не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_city = request.data.get("name")
        if not telegram_id or not new_city:
            return Response(
                {"detail": "Поля 'telegram_id' и 'name' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Client")
        except User.DoesNotExist:
            return Response(
                {"detail": "Клиент с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем город клиента
        user.city_name = new_city
        user.save()
        return Response(
            {"detail": f"Город клиента обновлён на '{new_city}'."},
            status=status.HTTP_200_OK
        )
    
class MasterServiceUpdateView(APIView):
    """
    API‑точка для обновления вида услуг мастера.
    Во входных данных ожидаются:
      - telegram_id: Telegram ID мастера
      - name: новое название услуги мастера
    """
    @swagger_auto_schema(
        operation_description="Обновляет вид услуг мастера. Принимает telegram_id и name (новое название услуги).",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID мастера"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новое название услуги мастера"
                )
            },
            required=["telegram_id", "name"]
        ),
        responses={
            200: openapi.Response(
                description="Вид услуг мастера обновлён",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Мастер или его профиль не найдены",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            )
        }
    )
    def post(self, request):
        telegram_id = request.data.get("telegram_id")
        new_service_name = request.data.get("name")
        if not telegram_id or not new_service_name:
            return Response(
                {"detail": "Поля 'telegram_id' и 'name' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            # Ищем пользователя с ролью "Master"
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response(
                {"detail": "Мастер с данным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        try:
            master = user.master_profile
        except Master.DoesNotExist:
            return Response(
                {"detail": "Профиль мастера не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем вид услуг мастера
        master.service_name = new_service_name
        master.save()
        return Response(
            {"detail": f"Вид услуг мастера обновлён на '{new_service_name}'."},
            status=status.HTTP_200_OK
        )
    

class AmoCRMContactUpdateView(APIView):
    """
    API‑точка для обновления данных контакта из AmoCRM.
    Принимает POST‑запрос с данными контакта.
    Клиент определяется по его amo_crm_contact_id.
    Возможные поля для обновления:
      - name: новое имя контакта (опционально)
      - phone: новый номер телефона (опционально)
      - city_name: новый город контакта (опционально)
    """
    @swagger_auto_schema(
        operation_description="Обновляет данные контакта (имя, телефон, город) на основании amo_crm_contact_id.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "amo_crm_contact_id": openapi.Schema(
                    type=openapi.TYPE_INTEGER,
                    description="ID контакта в AmoCRM"
                ),
                "name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новое имя контакта (опционально)"
                ),
                "phone": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новый номер телефона контакта (опционально)"
                ),
                "city_name": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Новый город контакта (опционально)"
                )
            },
            required=["amo_crm_contact_id"]
        ),
        responses={
            200: openapi.Response(
                description="Данные контакта успешно обновлены",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Контакт с данным amo_crm_contact_id не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        # Выводим входное сообщение в лог
        logger.info(f"Received AmoCRM update data: {data}")
        
        amo_crm_contact_id = data.get("amo_crm_contact_id")
        if amo_crm_contact_id is None:
            return Response(
                {"detail": "Параметр 'amo_crm_contact_id' обязателен."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            user = User.objects.get(amo_crm_contact_id=amo_crm_contact_id)
        except User.DoesNotExist:
            return Response(
                {"detail": f"Контакт с amo_crm_contact_id={amo_crm_contact_id} не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        # Обновляем данные, если они переданы
        updated_fields = []
        name = data.get("name")
        phone = data.get("phone")
        city_name = data.get("city_name")
        if name is not None:
            user.name = name
            updated_fields.append("name")
        if phone is not None:
            user.phone = phone
            updated_fields.append("phone")
        if city_name is not None:
            user.city_name = city_name
            updated_fields.append("city_name")
        user.save()
        return Response(
            {"detail": f"Данные контакта обновлены. Обновленные поля: {', '.join(updated_fields)}."},
            status=status.HTTP_200_OK
        )


def stars_to_int(star_string):
    """
    Преобразует входную строку, содержащую цифру и символ звездочки (например, "1⭐"),
    возвращая только числовое значение.
    Если цифры нет, возвращает 0.
    """
    if not star_string:
        return 0
    # Извлекаем все цифры из строки и объединяем их в одну строку
    digit_str = ''.join(filter(str.isdigit, star_string))
    try:
        return int(digit_str)
    except ValueError:
        return 0



class UpdateServiceRequestRatingView(APIView):
    """
    API‑точка для обновления рейтинговых параметров заявки.
    Принимает request_id и три рейтинговых параметра, представленных в виде строк,
    содержащих цифру и символ звездочки (например, "1⭐" для рейтинга 1).
    """
    @swagger_auto_schema(
        operation_description="Обновляет рейтинговые параметры заявки по request_id. "
                              "Рейтинги принимаются в виде строк, содержащих цифру и символ звездочки (например, '1⭐').",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "request_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="ID заявки (например, 'Оставить отзыв айди заявки 12312312312')"
                ),
                "quality_rating": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Качество работ (например, '1⭐')"
                ),
                "competence_rating": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Компетентность мастера (например, '1⭐')"
                ),
                "recommendation_rating": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Готовность рекомендовать (например, '1⭐')"
                )
            },
            required=["request_id", "quality_rating", "competence_rating", "recommendation_rating"]
        ),
        responses={
            200: openapi.Response(
                description="Рейтинги обновлены успешно",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        ),
                        "request_id": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Извлечённый ID заявки"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Заявка не найдена",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        raw_request_id = data.get("request_id")
        quality_rating_str = data.get("quality_rating")
        competence_rating_str = data.get("competence_rating")
        recommendation_rating_str = data.get("recommendation_rating")
        
        if not raw_request_id or quality_rating_str is None or competence_rating_str is None or recommendation_rating_str is None:
            return Response({"detail": "Параметры 'request_id' и все три рейтинга обязательны."},
                            status=status.HTTP_400_BAD_REQUEST)
        
        # 1. Извлекаем числовую часть ID заявки
        match = re.search(r"(\d+)$", raw_request_id)
        if not match:
            return Response({"detail": "Не удалось извлечь ID заявки из входных данных."},
                            status=status.HTTP_400_BAD_REQUEST)
        request_id = match.group(1)
        
        # 2. Ищем заявку
        try:
            service_request = ServiceRequest.objects.get(amo_crm_lead_id=request_id)
        except ServiceRequest.DoesNotExist:
            return Response({"detail": f"Заявка с request_id {request_id} не найдена."},
                            status=status.HTTP_404_NOT_FOUND)
        
        # 3. Преобразуем строки "1⭐" => int(1..5)
        quality_value = stars_to_int(quality_rating_str)
        competence_value = stars_to_int(competence_rating_str)
        recommendation_value = stars_to_int(recommendation_rating_str)
        
        # 4. Проверяем диапазон от 1 до 5
        if not (1 <= quality_value <= 5 and 1 <= competence_value <= 5 and 1 <= recommendation_value <= 5):
            return Response({"detail": "Все рейтинговые параметры должны быть в диапазоне от 1 до 5."},
                            status=status.HTTP_400_BAD_REQUEST)
        
        # 5. Сохраняем рейтинги в базе
        service_request.quality_rating = quality_value
        service_request.competence_rating = competence_value
        service_request.recommendation_rating = recommendation_value
        service_request.save(update_fields=["quality_rating", "competence_rating", "recommendation_rating"])
        
        # 6. Пересчитываем рейтинг мастера (если есть)
        if service_request.master:
            recalc_master_rating(service_request.master)

        # 7. Теперь обновим поля рейтинга в AmoCRM
        #    ID полей, по вашему указанию: 
        #    Качество работ = 748771, Компетентность = 748773, Рекомендовать = 748775
        lead_id = service_request.amo_crm_lead_id
        if lead_id:
            try:
                amocrm_client = AmoCRMClient()
                amocrm_client.update_lead(
                    lead_id,
                    {
                        "custom_fields_values": [
                            {
                                "field_id": 748771,
                                "values": [{"value": str(quality_value)}]
                            },
                            {
                                "field_id": 748773,
                                "values": [{"value": str(competence_value)}]
                            },
                            {
                                "field_id": 748775,
                                "values": [{"value": str(recommendation_value)}]
                            }
                        ]
                    }
                )
            except Exception as e:
                logger.error(f"Не удалось обновить рейтинги в AmoCRM для сделки {lead_id}: {e}")

        return Response(
            {
                "detail": "Рейтинги успешно обновлены.",
                "request_id": request_id
            },
            status=status.HTTP_200_OK
        )


class MasterBalanceView(APIView):
    """
    API‑точка для запроса параметров баланса мастера.
    Принимает POST‑запрос с параметром telegram_id мастера
    и возвращает следующие параметры:
      - name: Имя мастера
      - balance: Текущий баланс (без запятых)
      - status: Статус мастера ("Мастер")
      - commission: Комиссия за заявку (например, "30%") – берется из ServiceType
      - first_level_invites: Количество приглашённых мастеров 1 уровня
      - second_level_invites: Количество приглашённых мастеров 2 уровня
      - total_invites: Общее число приглашённых мастеров (1-го и 2-го уровней)
      - service_type: Вид услуги (значение поля service_name мастера)
      - task_of_day: Рекомендация для перехода на следующий уровень
    """
    @swagger_auto_schema(
        operation_description="Возвращает параметры баланса мастера и информацию о партнёрской программе.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Telegram ID мастера"
                )
            },
            required=["telegram_id"]
        ),
        responses={
            200: openapi.Response(
                description="Параметры успешно получены",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "name": openapi.Schema(type=openapi.TYPE_STRING, description="Имя мастера"),
                        "balance": openapi.Schema(type=openapi.TYPE_STRING, description="Текущий баланс (без запятых)"),
                        "status": openapi.Schema(type=openapi.TYPE_STRING, description="Статус мастера"),
                        "commission": openapi.Schema(type=openapi.TYPE_STRING, description="Комиссия за заявку"),
                        "first_level_invites": openapi.Schema(type=openapi.TYPE_INTEGER, description="Приглашённые мастера 1 уровня"),
                        "second_level_invites": openapi.Schema(type=openapi.TYPE_INTEGER, description="Приглашённые мастера 2 уровня"),
                        "total_invites": openapi.Schema(type=openapi.TYPE_INTEGER, description="Общее количество приглашённых мастеров"),
                        "service_type": openapi.Schema(type=openapi.TYPE_STRING, description="Вид услуги мастера"),
                        "task_of_day": openapi.Schema(type=openapi.TYPE_STRING, description="Рекомендация для перехода на следующий уровень")
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Мастер не найден",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get("telegram_id")
        if not telegram_id:
            return Response({"detail": "Параметр 'telegram_id' обязателен."}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response({"detail": f"Мастер с telegram_id {telegram_id} не найден."}, status=status.HTTP_404_NOT_FOUND)
        
        try:
            master = user.master_profile
        except Exception:
            return Response({"detail": "Профиль мастера не найден."}, status=status.HTTP_404_NOT_FOUND)
        
        # Форматируем баланс – число без запятых (например, "4500.00")
        balance = int(master.balance)
        
        # Определяем комиссию за заявку по типу сервиса мастера.
        commission = "Нет данных"
        if master.service_name:
            service_type_obj = ServiceType.objects.filter(name=master.service_name).first()
            if service_type_obj:
                if master.level == 1:
                    commission_value = service_type_obj.commission_level_1 or 0
                elif master.level == 2:
                    commission_value = service_type_obj.commission_level_2 or 0
                elif master.level == 3:
                    commission_value = service_type_obj.commission_level_3 or 0
                else:
                    commission_value = service_type_obj.commission_level_1 or 0
                commission = f"{int(commission_value)}%"
        
        # Подсчитываем приглашённых мастеров первого уровня
        first_level_invites = User.objects.filter(referrer=user, role="Master").count()
        # Приглашённые мастера второго уровня: те, кого приглашают пользователи первого уровня
        first_level_users = User.objects.filter(referrer=user, role="Master")
        second_level_invites = User.objects.filter(referrer__in=first_level_users, role="Master").count()
        total_invites = first_level_invites + second_level_invites
        
        # Рекомендация для задачи дня
        task_of_day = get_task_of_day(master)
        
        # Вид услуги, указанный у мастера
        service_type_str = master.service_name if master.service_name else "Нет данных"
        
        response_data = {
            "name": user.name,
            "balance": balance,
            "status": "Мастер",
            "commission": commission,
            "first_level_invites": first_level_invites,
            "second_level_invites": second_level_invites,
            "total_invites": total_invites,
            "service_type": service_type_str,
            "task_of_day": task_of_day
        }
        return Response(response_data, status=status.HTTP_200_OK)


    


def get_task_of_day(master_profile):
    """
    Возвращает задание дня для мастера на основе его текущего уровня.
    Здесь успешными считаются заявки, завершённые со статусом 'Completed' и имеющие прикреплённый WorkOutcome с is_success=True.
    """
    # Подсчет успешных заявок с WorkOutcome с is_success=True
    completed_count = ServiceRequest.objects.filter(
        master=master_profile,
        status='Completed',
        work_outcome__is_success=True  # <-- вместо work_outcome_record__is_success=True
    ).count()
    closed_count = ServiceRequest.objects.filter(master=master_profile, status='Closed').count()
    # Переменная difference – это разница между количеством успешных заявок и закрытых заявок.
    # Она служит показателем чистой успешности мастера: чем выше difference, тем лучше.
    difference = completed_count - closed_count

    # Количество приглашённых мастеров с депозитом
    invited_with_deposit = count_invited_masters_with_deposit(master_profile.user)
    current_level = master_profile.level

    settings_obj = Settings.objects.first()
    if settings_obj:
        req_orders_level2 = settings_obj.required_orders_level2
        req_invites_level2 = settings_obj.required_invites_level2
        req_orders_level3 = settings_obj.required_orders_level3
        req_invites_level3 = settings_obj.required_invites_level3
    else:
        req_orders_level2, req_invites_level2 = 10, 1
        req_orders_level3, req_invites_level3 = 30, 3

    if current_level == 3:
        return "Вы достигли максимального уровня. Поздравляем!"
    elif current_level == 1:
        needed_orders = max(0, req_orders_level2 - difference)
        needed_invites = max(0, req_invites_level2 - invited_with_deposit)
        tasks = []
        if needed_orders > 0:
            tasks.append(f"выполните ещё {needed_orders} заказ{'ов' if needed_orders != 1 else ''}")
        if needed_invites > 0:
            tasks.append(f"пригласите ещё {needed_invites} мастера")
        if tasks:
            return " и ".join(tasks) + " для перехода на уровень 2."
        else:
            return "Вы готовы перейти на уровень 2! Поздравляем!"
    elif current_level == 2:
        needed_orders = max(0, req_orders_level3 - difference)
        needed_invites = max(0, req_invites_level3 - invited_with_deposit)
        tasks = []
        if needed_orders > 0:
            tasks.append(f"выполните ещё {needed_orders} заказ{'ов' if needed_orders != 1 else ''}")
        if needed_invites > 0:
            tasks.append(f"пригласите ещё {needed_invites} мастера")
        if tasks:
            return " и ".join(tasks) + " для перехода на уровень 3."
        else:
            return "Вы готовы перейти на уровень 3! Поздравляем!"
        

class ClientReviewUpdateView(APIView):
    """
    API‑точка для обновления отзыва клиента.
    Принимает POST‑запрос с полями:
      - request_id: текст, содержащий ID заявки (например, "Оставить отзыв 24859199")
      - client_review: текст отзыва клиента
    После обновления возвращает сообщение об успешном сохранении.
    """

    @swagger_auto_schema(
        operation_description="Обновляет отзыв клиента для заявки по request_id.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "request_id": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Текст с ID заявки (например, 'Оставить отзыв 24859199')"
                ),
                "client_review": openapi.Schema(
                    type=openapi.TYPE_STRING,
                    description="Текст отзыва клиента"
                )
            },
            required=["request_id", "client_review"]
        ),
        responses={
            200: openapi.Response(
                description="Отзыв клиента успешно обновлен.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Сообщение об успешном обновлении"
                        ),
                        "request_id": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Извлечённый ID заявки"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            404: openapi.Response(
                description="Заявка не найдена.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            )
        }
    )
    def post(self, request):
        data = request.data
        raw_request_id = data.get("request_id")
        client_review_text = data.get("client_review")
        
        if not raw_request_id or client_review_text is None:
            return Response(
                {"detail": "Поля 'request_id' и 'client_review' обязательны."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Извлекаем числовую последовательность в конце строки
        match = re.search(r"(\d+)$", raw_request_id)
        if not match:
            return Response({"detail": "Не удалось извлечь ID заявки из входных данных."}, status=status.HTTP_400_BAD_REQUEST)
        extracted_id = match.group(1)
        
        try:
            service_request = ServiceRequest.objects.get(amo_crm_lead_id=extracted_id)
        except ServiceRequest.DoesNotExist:
            return Response(
                {"detail": f"Заявка с ID {extracted_id} не найдена."},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Сохраняем отзыв в модель
        service_request.client_review = client_review_text
        service_request.save(update_fields=["client_review"])
        
        # ==== Дополняем код: Обновляем поле отзыва в AmoCRM (ID поля 748949) ====
        lead_id = service_request.amo_crm_lead_id
        if lead_id:
            try:
                amocrm_client = AmoCRMClient()
                amocrm_client.update_lead(
                    lead_id,
                    {
                        "custom_fields_values": [
                            {
                                "field_id": 748949,
                                "values": [{"value": client_review_text}]
                            }
                        ]
                    }
                )
            except Exception as e:
                logger.error(f"Не удалось обновить поле отзыва в AmoCRM для сделки {lead_id}: {e}")
        # ================================================================

        return Response(
            {"detail": "Отзыв клиента успешно обновлён.", "request_id": extracted_id},
            status=status.HTTP_200_OK
        )

import time
# Например, вверху файла views.py
group_check_results = {}  # dict[telegram_id: bool], где True/False = вступил/не вступил


def check_master_in_group(telegram_id: str) -> bool:
    """
    Запрашивает у SamBot проверку, вступил ли мастер в группу.
    Ждёт до 10 секунд, пока SamBot отправит колбэк в MasterGroupCheckCallbackView.
    Возвращает True, если в итоге joined = True, иначе False.
    """
    # (1) Готовим URL SamBot. Предположим, token тот же:
    url = "https://sambot.ru/reactions/3011532/start?token=yhvtlmhlqbj"

    # (2) Передаём данные, в том числе callback_url:
    #  callback_url — это ваш эндпоинт, где вы ожидаете joined: true/false
    payload = {
       "telegram_id": telegram_id,  
    }

    # (3) Посылаем запрос
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Ошибка при запросе к SamBot: {e}")
        return False

    # (4) Очищаем предыдущее состояние
    if telegram_id in group_check_results:
        del group_check_results[telegram_id]

    # (5) Ждём до 10 секунд, пока SamBot не стукнется колбэком
    total_wait = 10
    for _ in range(total_wait):
        time.sleep(1)
        # Проверяем, пришёл ли ответ
        if telegram_id in group_check_results:
            return group_check_results[telegram_id]

    # Если за 10 секунд колбэк не пришёл, считаем, ч


class MasterGroupCheckCallbackView(APIView):
    """
    Колбэк, в который SamBot шлёт "joined: true/false" после проверки вступления в группу.
    """
    def post(self, request):
        data = request.data
        telegram_id = data.get("telegram_id")
        joined = data.get("joined")

        if not telegram_id or joined is None:
            return Response({"detail": "Поля telegram_id и joined обязательны"}, status=400)

        # Сохраняем в глобальный словарь
        group_check_results[telegram_id] = bool(joined)  # приведение к bool

        return Response({"detail": "OK, status saved"}, status=200)


class MasterGroupMembershipUpdateView(APIView):
    """
    API‑точка для обновления признака вступления в группу для мастера.
    Принимает POST‑запрос с полями:
      - telegram_id: Telegram ID мастера
      - joined_group: булевое значение (True, если мастер вступил в группу, иначе False)
    """
    @swagger_auto_schema(
        operation_description="Обновляет признак вступления в группу для мастера.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(type=openapi.TYPE_STRING, description="Telegram ID мастера"),
                "joined_group": openapi.Schema(type=openapi.TYPE_BOOLEAN, description="True, если мастер вступил в группу")
            },
            required=["telegram_id", "joined_group"]
        ),
        responses={
            200: openapi.Response(
                description="Признак вступления для мастера обновлён.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные.",
                schema=openapi.Schema(type=openapi.TYPE_OBJECT)
            ),
            404: openapi.Response(
                description="Мастер или профиль не найдены.",
                schema=openapi.Schema(type=openapi.TYPE_OBJECT)
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get("telegram_id")
        joined_group = data.get("joined_group")
        if telegram_id is None or joined_group is None:
            return Response({"detail": "Поля 'telegram_id' и 'joined_group' обязательны."},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response({"detail": "Мастер с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)
        try:
            master = user.master_profile
        except Master.DoesNotExist:
            return Response({"detail": "Профиль мастера не найден."},
                            status=status.HTTP_404_NOT_FOUND)
        master.joined_group = bool(joined_group)
        master.save(update_fields=["joined_group"])
        return Response({"detail": "Признак вступления в группу для мастера обновлён."},
                        status=status.HTTP_200_OK)


class ClientGroupMembershipUpdateView(APIView):
    """
    API‑точка для обновления признака вступления в группу для клиента.
    Принимает POST‑запрос с полями:
      - telegram_id: Telegram ID клиента
      - joined_group: булевое значение (True, если клиент вступил в группу, иначе False)
    """
    @swagger_auto_schema(
        operation_description="Обновляет признак вступления в группу для клиента.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "telegram_id": openapi.Schema(type=openapi.TYPE_STRING, description="Telegram ID клиента"),
                "joined_group": openapi.Schema(type=openapi.TYPE_BOOLEAN, description="True, если клиент вступил в группу")
            },
            required=["telegram_id", "joined_group"]
        ),
        responses={
            200: openapi.Response(
                description="Признак вступления для клиента обновлён.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={"detail": openapi.Schema(type=openapi.TYPE_STRING)}
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные.",
                schema=openapi.Schema(type=openapi.TYPE_OBJECT)
            ),
            404: openapi.Response(
                description="Клиент не найден.",
                schema=openapi.Schema(type=openapi.TYPE_OBJECT)
            )
        }
    )
    def post(self, request):
        data = request.data
        telegram_id = data.get("telegram_id")
        joined_group = data.get("joined_group")
        if telegram_id is None or joined_group is None:
            return Response({"detail": "Поля 'telegram_id' и 'joined_group' обязательны."},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Client")
        except User.DoesNotExist:
            return Response({"detail": "Клиент с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)
        user.joined_group = bool(joined_group)
        user.save(update_fields=["joined_group"])
        return Response({"detail": "Признак вступления в группу для клиента обновлён."},
                        status=status.HTTP_200_OK)
