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
from django.db.models import Sum
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
from .models import EquipmentType, Master, RatingLog, ReferralLink, ServiceRequest, ServiceType, Settings, Transaction, User

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
            user = User.objects.get(telegram_id=telegram_id)
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
    Пересчитывает рейтинг мастера как среднее значение по всем заявкам,
    в которых заданы все три рейтинговых поля.
    """
    from decimal import Decimal
    requests_qs = ServiceRequest.objects.filter(
        master=master,
        quality_rating__isnull=False,
        competence_rating__isnull=False,
        recommendation_rating__isnull=False
    )
    if not requests_qs.exists():
        return
    total = Decimal('0.0')
    count = 0
    for req in requests_qs:
        # Вычисляем среднее значение по заявке
        avg_req = (req.quality_rating + req.competence_rating + req.recommendation_rating) / 3
        total += Decimal(avg_req)
        count += 1
    master.rating = total / count if count > 0 else Decimal('0.0')
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

                with transaction.atomic():
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
                    service_request.save()

                    if service_request.master:
                        recalc_master_rating(service_request.master)

                    # Определяем новое имя статуса по STATUS_MAPPING
                    status_name = None
                    for k, v in STATUS_MAPPING.items():
                        if v == new_status_id:
                            status_name = k
                            break
                    if not status_name:
                        logger.warning(f"No matching status found for status_id={new_status_id}")
                        continue

                    update_fields = {
                        'status': status_name,
                        'amo_status_code': new_status_id,
                    }
                    previous_status = service_request.status

                    # Обрабатываем статусы: AwaitingClosure, Completed, QualityControl
                    if status_name in ['AwaitingClosure', 'Completed', 'QualityControl']:
                        # Если входящее поле price передано, проверяем и обновляем комиссию
                        if incoming_price is not None:
                            new_price_val = Decimal(incoming_price)
                            if service_request.price != new_price_val:
                                diff = update_commission_transaction(service_request, incoming_price)
                                update_fields['price'] = new_price_val
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
                        for field, value in update_fields.items():
                            setattr(service_request, field, value)
                        fields_to_update = ['status', 'amo_status_code']
                        if 'price' in update_fields:
                            fields_to_update.append('price')
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
                            # Передаем параметр skip_commission=True, чтобы handle_completed_deal не создавал комиссию повторно
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
            except ServiceRequest.DoesNotExist:
                logger.error(f"ServiceRequest with amo_crm_lead_id={lead_id} does not exist.")
                continue
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
    Подбирает мастеров в зависимости от круга рассылки,
    исключая неактивных мастеров.
    """
    service_request = ServiceRequest.objects.get(id=service_request_id)

    city_name = service_request.city_name.lower()
    equipment_type = (service_request.equipment_type or "").lower()

    # Выбираем мастеров, у которых пользователь активен
    masters = Master.objects.select_related('user').filter(user__is_active=True)
    selected_masters = []

    now_time = now()
    last_24_hours = now_time - timedelta(hours=24)

    for master in masters:
        master_cities = (master.city_name or "").lower()
        master_equips = (master.equipment_type_name or "").lower()

        if city_name in master_cities and equipment_type in master_equips:
            success_ratio, cost_ratio, last_deposit = get_master_statistics(master)

            if round_num == 1 and success_ratio >= 0.8 and cost_ratio <= 0.3 and last_deposit >= last_24_hours:
                selected_masters.append(master.user.telegram_id)
            elif round_num == 2 and success_ratio >= 0.8 and 0.3 < cost_ratio <= 0.5:
                selected_masters.append(master.user.telegram_id)
            elif round_num == 3:
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
        user=master.user, transaction_type="Deposit", status="Confirmed"
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


def handle_completed_deal(service_request, operator_comment, previous_status, lead_id):
    """
    Обработка сделки со статусом 'Completed':
    1) Считаем комиссию из ServiceType по имени
    2) Создаем транзакцию с типом Comission
    3) Отправляем POST на sambot
    4) Обрабатываем итог работы: если итог - штраф, создаем транзакцию с типом Penalty,
       и сохраняем итог работы для ServiceRequest.
    5) Пересчитываем уровень мастера (повышение / понижение)
    """
    from decimal import Decimal
    import requests
    import logging
    from .models import ServiceType, WorkOutcome, Transaction

    logger = logging.getLogger(__name__)

    # 1) Получаем сумму сделки
    deal_amount = service_request.price or Decimal('0.00')
    deal_amount = deal_amount - service_request.spare_parts_spent

    # Получаем Master (если нет мастера - пропускаем)
    master_profile = service_request.master
    if not master_profile:
        logger.warning(
            "ServiceRequest %s: no master assigned, skipping commission",
            service_request.id
        )
        return

    # Текущий уровень мастера (1, 2 или 3)
    master_level = master_profile.level

    service_type_name = service_request.service_name  # Имя сервиса из текстового поля
    service_type = None
    if service_type_name:
        service_type = ServiceType.objects.filter(name=service_type_name).first()

    # 3) Определяем процент комиссии, если нашли нужный ServiceType
    if not service_type:
        logger.warning(
            "ServiceRequest %s: ServiceType with name='%s' not found. Commission = 0 by default.",
            service_request.id,
            service_type_name
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

    
    # 4) Рассчитываем сумму комиссии
    commission_amount = deal_amount * commission_percentage / Decimal('100')

    # 5) Создаем транзакцию для комиссии с типом "Comission" и статусом Confirmed.
    # Автоматический пересчет баланса произойдет через сигналы.
    Transaction.objects.create(
        user=master_profile.user,  # Здесь предполагается, что транзакция для мастера привязана через поле user (или master, если используется обновленная схема)
        amount=commission_amount,
        transaction_type='Comission',
        status='Confirmed'
    )

    # 6) Отправляем POST на sambot
    payload = {
        "request_id": lead_id,
        "telegram_id": master_profile.user.telegram_id if master_profile else "",
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
            logger.error(
                f"Failed to send data (Completed) for Request {service_request.id}. "
                f"Status code: {response_sambot.status_code}, Response: {response_sambot.text}"
            )
    except Exception as ex:
        logger.error(f"Error sending data (Completed) to sambot: {ex}")

    # 7) Обработка итога работы по полю deal_success.
    if service_request.deal_success:
        outcome_record = WorkOutcome.objects.filter(outcome_name=service_request.deal_success).first()
        if outcome_record:
            if outcome_record.is_penalty:
                penalty_amount = outcome_record.penalty_amount
                # Создаем транзакцию для штрафа с типом "Penalty" и статусом Confirmed
                Transaction.objects.create(
                    user=master_profile.user,
                    amount=penalty_amount,
                    transaction_type='Penalty',
                    status='Confirmed'
                )
                logger.info(f"Penalty applied: {penalty_amount} recorded for master {master_profile.user.id}.")
            # Привязываем итог работы к ServiceRequest
            service_request.work_outcome = outcome_record
            service_request.save()
            logger.info(f"Work outcome '{outcome_record.outcome_name}' attached to ServiceRequest {service_request.id}.")
        else:
            logger.warning(f"WorkOutcome with name '{service_request.deal_success}' not found for ServiceRequest {service_request.id}.")

    # 8) Пересчитываем уровень мастера
    recalc_master_level(master_profile)


def recalc_master_level(master_profile):
    """
    Пересчитывает уровень мастера на основе:
    1) (Completed - Closed) заявок
    2) Сколько мастер пригласил мастеров, у которых есть хотя бы один Confirmed депозит.
    3) Правила повышения/понижения уровня (1->2->3)
    """

    user = master_profile.user
    current_level = master_profile.level

    # 1) Подсчёт заявок
    completed_count = ServiceRequest.objects.filter(master=master_profile, status='Completed').count()
    closed_count = ServiceRequest.objects.filter(master=master_profile, status='Closed').count()
    difference = completed_count - closed_count

    # 2) Подсчёт, сколько мастер пригласил Мастеров, имеющих хотя бы 1 пополнение
    invited_with_deposit = count_invited_masters_with_deposit(user)

    # Условия для уровней:
    #   Уровень 2: difference >= 10, invited_with_deposit >= 1
    #   Уровень 3: difference >= 30, invited_with_deposit >= 3
    #
    # Для понижения: если текущий уровень 2, но difference < 8 (80% от 10)
    #                или invited_with_deposit < 1, => падаем на 1
    #
    #               если текущий уровень 3, но difference < 24 (80% от 30)
    #                или invited_with_deposit < 3 => пробуем условия уровня 2,
    #                если тоже не подходит => уровень 1.

    new_level = current_level  # по умолчанию оставляем

    # === Проверяем повышение ===
    # Сначала проверяем возможность достичь 3
    if difference >= 30 and invited_with_deposit >= 3:
        new_level = 3
    # иначе пробуем достичь 2
    elif difference >= 10 and invited_with_deposit >= 1:
        new_level = 2
    else:
        new_level = 1

    # === Проверяем «не дотягивает» ли до текущего уровня (понижение) ===
    # Если мастер уже 3, но difference <24 или invited_with_deposit<3 => пробуем уровень 2, если не выйдет ->1
    if current_level == 3:
        if difference < 24 or invited_with_deposit < 3:
            # пытаемся удержаться на уровне 2
            if difference >= 10 and invited_with_deposit >= 1:
                new_level = 2
            else:
                new_level = 1
    # Если мастер 2, но difference <8 или invited_with_deposit <1 => уровень 1
    elif current_level == 2:
        if difference < 8 or invited_with_deposit < 1:
            new_level = 1

    # Сохраняем, если изменилось
    if new_level != current_level:
        master_profile.level = new_level
        master_profile.save()
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
        transaction__transaction_type='Deposit',
        transaction__status='Confirmed'
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
            user = User.objects.get(telegram_id=telegram_id)
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
                        "current_status": openapi.Schema(type=openapi.TYPE_STRING),
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
        """
        Пример: POST /api/master_stats/
        Тело запроса: { "telegram_id": "12345" }
        Возвращает JSON-объект со статистикой мастера и текстовым полем top_10 мастеров.
        """
        data = request.data
        telegram_id = data.get('telegram_id')

        if not telegram_id:
            return Response(
                {"detail": "Поле telegram_id обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # 1) Проверяем пользователя
        try:
            user = User.objects.get(telegram_id=telegram_id, role="Master")
        except User.DoesNotExist:
            return Response({"detail": "Мастер с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)

        # Предполагаем, что у пользователя role='Master' и есть master_profile
        master = getattr(user, 'master_profile', None)
        if not master:
            return Response({"detail": "Пользователь не является мастером."},
                            status=status.HTTP_404_NOT_FOUND)

        # -----------------------------------
        # Вычисляем статистику мастера
        # -----------------------------------
        finished_statuses = ['Completed', 'AwaitingClosure', 'Closed', 'QualityControl']
        completed_qs = ServiceRequest.objects.filter(master=master, status__in=finished_statuses)

        # Количество выполненных заявок
        completed_orders_count = completed_qs.count()

        # Сумма дохода
        total_income_value = completed_qs.aggregate(sum_price=Sum('price'))['sum_price'] or Decimal("0")

        # Рейтинг
        master_rating = master.rating or Decimal("0.0")

        # Среднее время (end_date - start_date)
        avg_time_seconds = 0
        count_for_avg = 0
        for req in completed_qs:
            if req.start_date and req.end_date:
                delta = req.end_date - req.start_date
                avg_time_seconds += delta.total_seconds()
                count_for_avg += 1
        if count_for_avg > 0:
            avg_seconds = avg_time_seconds / count_for_avg
        else:
            avg_seconds = 0
        avg_hours = int(avg_seconds // 3600)

        # Остальные поля (заглушки или вычисления)
        quality_percent_str = "95%"
        balance_topup_speed_str = "12 часов"
        cost_percentage_str = "15%"
        current_status_str = "1-й круг"
        rating_place_str = "—"

        registration_date = user.created_at.strftime("%d.%m.%Y") if user.created_at else "—"

        data_for_master = {
            "fio": user.name,
            "registration_date": registration_date,
            "rating": f"{master_rating}⭐️",
            "completed_orders": completed_orders_count,
            "avg_time": f"{avg_hours} часов",
            "total_income": f"{int(total_income_value)} руб.",
            "quality_percent": quality_percent_str,
            "balance_topup_speed": balance_topup_speed_str,
            "cost_percentage": cost_percentage_str,
            "current_status": current_status_str,
            "rating_place": rating_place_str,
        }

        # -----------------------------------
        # Реальный ТОП-10 мастеров (доход по завершённым заявкам)
        # -----------------------------------
        all_masters = Master.objects.all()
        stats_list = []

        for m in all_masters:
            m_finished_qs = ServiceRequest.objects.filter(master=m, status__in=finished_statuses)
            m_income = m_finished_qs.aggregate(sum_price=Sum('price'))['sum_price'] or Decimal("0")
            m_rating = m.rating or Decimal("0.0")
            m_cities = m.city_name or ""
            stats_list.append((m, m_income, m_rating, m_cities))

        # Сортируем по доходу убыванием
        stats_list.sort(key=lambda x: x[1], reverse=True)

        # Найдём место запрашиваемого мастера
        for idx, item in enumerate(stats_list, start=1):
            if item[0].id == master.id:
                data_for_master["rating_place"] = f"{idx} место"
                break

        # Берём первые 10
        top_10_data = stats_list[:10]

        # Формируем одну многострочную строку
        lines = []
        for idx, (m, inc, rat, cts) in enumerate(top_10_data, start=1):
            # Пример формата: 
            # 1.| Чеблаков Алексей Юрьевич| Ульяновск димитровград новоульяновск| 159240 руб.| 5⭐️
            line = f"{idx}.| {m.user.name}| {cts}| {int(inc)} руб.| {rat}⭐️"
            lines.append(line)

        top_10_str = "\n\n".join(lines)  # можно сделать "\n".join(lines) если нужен перенос без пустой строки

        # -----------------------------------
        # Формируем общий ответ
        # -----------------------------------
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
            user = User.objects.get(telegram_id=telegram_id, role="Master"),
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
                user=user,
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

            # Увеличиваем баланс мастера
            user = tx.user
            master = getattr(user, 'master_profile', None)
            if not master:
                return Response({"detail": "Пользователь не является мастером."}, status=status.HTTP_404_NOT_FOUND)

            master.save()

            # Проверяем, является ли это первое пополнение
            first_deposit = not Transaction.objects.filter(user=user, transaction_type='Deposit', status='Confirmed').exclude(id=tx.id).exists()

            # Если это **первое пополнение**, начисляем бонус
            if first_deposit:
                ref_1 = user.referrer  # первая линия
                if ref_1 and ref_1.role == 'Master':
                    ref_1.master_profile.balance += Decimal(500)
                    ref_1.master_profile.save()

                    # проверяем вторую линию
                    ref_2 = ref_1.referrer
                    if ref_2 and ref_2.role == 'Master':
                        ref_2.master_profile.balance += Decimal(250)
                        ref_2.master_profile.save()

            return Response({
                "detail": "Транзакция подтверждена, баланс мастера обновлён. "
                          f"{'Бонусы начислены.' if first_deposit else 'Бонусы НЕ начислены (не первое пополнение).' }",
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
    Во входных данных требуется указать telegram_id.
    Если под одним telegram_id существуют записи для клиента и мастера,
    будет выбрана именно запись с ролью "Master".
    """
    @swagger_auto_schema(
        operation_description="Возвращает данные профиля мастера с расчетом оставшихся работ до следующего уровня.",
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
                description="Данные профиля мастера",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "message": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Форматированное сообщение профиля мастера"
                        ),
                        "level": openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description="Наименование уровня мастера"
                        )
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные входные данные или пользователь не является мастером",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Мастер или профиль не найдены",
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
        if not telegram_id:
            return Response(
                {"detail": "Поле 'telegram_id' обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )
        try:
            # Ищем именно пользователя с ролью "Master"
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

        # Подсчитываем количество отзывов (например, число записей RatingLog)
        reviews_count = RatingLog.objects.filter(master=master).count()

        # Определяем настройки для расчёта условий перехода по уровням
        level_settings = {
            1: {
                "current_commission": "30%",
                "current_max_requests": "1",
                "next_commission": "25%",
                "next_max_requests": "3",
                "required_works": 10,
                "required_invites": 1,
            },
            2: {
                "current_commission": "25%",
                "current_max_requests": "3",
                "next_commission": "20%",
                "next_max_requests": "5",
                "required_works": 30,
                "required_invites": 3,
            },
            3: {
                "current_commission": "20%",
                "current_max_requests": "5",
                "next_commission": "–",
                "next_max_requests": "–",
                "required_works": 0,
                "required_invites": 0,
            }
        }
        # Если уровень мастера не задан или больше 3, считаем его уровнем 3
        current_level = master.level if master.level in level_settings else 3
        settings = level_settings[current_level]

        # Подсчитываем количество выполненных заказов (без учета диагностики)
        completed_orders = ServiceRequest.objects.filter(
            master=master,
            status='Completed'
        ).exclude(service_name__icontains="диагностика").count()

        if settings["required_works"] > 0:
            remaining_works = settings["required_works"] - completed_orders
            if remaining_works < 0:
                remaining_works = 0
            progress_percent = min(100, int((completed_orders / settings["required_works"]) * 100))
        else:
            remaining_works = 0
            progress_percent = 100

        # Подсчитываем количество приглашённых мастеров
        invited_masters_count = User.objects.filter(referrer=user, role="Master").count()
        if settings["required_invites"] > 0:
            remaining_invites = settings["required_invites"] - invited_masters_count
            if remaining_invites < 0:
                remaining_invites = 0
        else:
            remaining_invites = 0

        # Определяем наименование уровня по маппингу
        level_name = MASTER_LEVEL_MAPPING.get(current_level, "Мастер")

        # Формируем форматированное сообщение
        message = (
            f"📋 <b>Мой профиль</b>\n"
            f"✏️ Имя: {user.name}\n"
            f"📞 Телефон: {user.phone}\n"
            f"🏙 Город: {master.city_name}\n"
            f"⭐️ Рейтинг: {master.rating}\n"
            f"💬 Отзывы: {reviews_count}\n\n"
            f"🎖 Уровень: {level_name}\n"
            f"🚀 Прогресс: {progress_percent}%\n\n"
            f"<b>Награды и привилегии на вашем уровне:</b>\n"
            f"💸 Комиссия: {settings['current_commission']}\n"
            f"🔨 Брать {settings['current_max_requests']} заявку в работу\n\n"
            f"<b>Что вас ждёт на следующем уровне:</b>\n"
            f"💸 Уменьшение комиссия: {settings['next_commission']}\n"
            f"🔨 Брать {settings['next_max_requests']} заявку в работу\n\n"
            f"📈 <b>Развитие:</b>\n"
            f"🛠 Осталось выполнить работ: {remaining_works}\n"
            f"👤 Осталось пригласить мастеров: {remaining_invites}\n\n"
            f"🛠 <b>Виды работ:</b> {master.equipment_type_name}"
        )

        return Response({"message": message, "level": master.level, "city": master.city_name, "name": user.name, "equipment": master.equipment_type_name, "phone": user.phone}, status=status.HTTP_200_OK)
    

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
    Преобразует строку звездочек в целое число, считая количество символов '⭐'.
    """
    if not star_string:
        return 0
    return star_string.count("⭐")

class UpdateServiceRequestRatingView(APIView):
    """
    API‑точка для обновления рейтинговых параметров заявки.
    Принимает request_id и три рейтинговых параметра, представленных в виде строк звездочек 
    (например, "⭐⭐⭐⭐⭐" для рейтинга 5).
    """
    @swagger_auto_schema(
        operation_description="Обновляет рейтинговые параметры заявки по request_id. Рейтинги принимаются в виде строк из звездочек (например, '⭐⭐⭐⭐⭐').",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                "request_id": openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки (amo_crm_lead_id)"),
                "quality_rating": openapi.Schema(type=openapi.TYPE_STRING, description="Качество работ (звездочки)"),
                "competence_rating": openapi.Schema(type=openapi.TYPE_STRING, description="Компетентность мастера (звездочки)"),
                "recommendation_rating": openapi.Schema(type=openapi.TYPE_STRING, description="Готовность рекомендовать (звездочки)")
            },
            required=["request_id", "quality_rating", "competence_rating", "recommendation_rating"]
        ),
        responses={
            200: openapi.Response(
                description="Рейтинги обновлены успешно",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING, description="Сообщение об успешном обновлении"),
                        "request_id": openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки")
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
        request_id = data.get("request_id")
        quality_rating_str = data.get("quality_rating")
        competence_rating_str = data.get("competence_rating")
        recommendation_rating_str = data.get("recommendation_rating")
        
        if not request_id:
            return Response({"detail": "Параметр 'request_id' обязателен."}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            service_request = ServiceRequest.objects.get(amo_crm_lead_id=request_id)
        except ServiceRequest.DoesNotExist:
            return Response({"detail": f"Заявка с request_id {request_id} не найдена."}, status=status.HTTP_404_NOT_FOUND)
        
        # Преобразуем звездочную строку в число звезд
        quality_value = stars_to_int(quality_rating_str)
        competence_value = stars_to_int(competence_rating_str)
        recommendation_value = stars_to_int(recommendation_rating_str)
        
        # Проверяем, что рейтинговые значения в диапазоне от 1 до 5
        if not (1 <= quality_value <= 5 and 1 <= competence_value <= 5 and 1 <= recommendation_value <= 5):
            return Response({"detail": "Все рейтинговые параметры должны быть в диапазоне от 1 до 5 звезд."}, status=status.HTTP_400_BAD_REQUEST)
        
        service_request.quality_rating = quality_value
        service_request.competence_rating = competence_value
        service_request.recommendation_rating = recommendation_value
        service_request.save(update_fields=["quality_rating", "competence_rating", "recommendation_rating"])
        
        if service_request.master:
            recalc_master_rating(service_request.master)
        
        return Response({"detail": "Рейтинги успешно обновлены.", "request_id": request_id}, status=status.HTTP_200_OK)


class MasterBalanceView(APIView):
    """
    API‑точка для запроса параметров баланса мастера.
    Принимает POST‑запрос с параметром telegram_id мастера
    и возвращает следующие параметры:
      - name: Имя мастера
      - balance: Текущий баланс
      - status: Статус мастера ("Мастер")
      - commission: Комиссия за заявку (например, "30%") – берется из ServiceType
      - first_level_invites: Количество приглашённых мастеров 1 уровня
      - second_level_invites: Количество приглашённых мастеров 2 уровня
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
                        "balance": openapi.Schema(type=openapi.TYPE_STRING, description="Текущий баланс"),
                        "status": openapi.Schema(type=openapi.TYPE_STRING, description="Статус мастера"),
                        "commission": openapi.Schema(type=openapi.TYPE_STRING, description="Комиссия за заявку"),
                        "first_level_invites": openapi.Schema(type=openapi.TYPE_INTEGER, description="Приглашённые мастера 1 уровня"),
                        "second_level_invites": openapi.Schema(type=openapi.TYPE_INTEGER, description="Приглашённые мастера 2 уровня"),
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
        
        # Текущий баланс мастера (в виде строки)
        balance = str(master.balance)
        
        # Определяем комиссию за заявку по типу сервиса мастера.
        commission = "N/A"
        if master.service_name:
            service_type = ServiceType.objects.filter(name=master.service_name).first()
            if service_type:
                if master.level == 1:
                    commission_value = service_type.commission_level_1 or 0
                elif master.level == 2:
                    commission_value = service_type.commission_level_2 or 0
                elif master.level == 3:
                    commission_value = service_type.commission_level_3 or 0
                else:
                    commission_value = service_type.commission_level_1 or 0
                commission = f"{commission_value}%"
        
        # Приглашённые мастера первого уровня: те, кого приглашает сам мастер
        first_level_invites = User.objects.filter(referrer=user, role="Master").count()
        # Приглашённые мастера второго уровня: те, кого приглашают пользователи первого уровня
        first_level_users = User.objects.filter(referrer=user, role="Master")
        second_level_invites = User.objects.filter(referrer__in=first_level_users, role="Master").count()
        
        # Рекомендация для задачи дня – условия формирования берутся из функции get_task_of_day (которая может использовать данные из Settings)
        task_of_day = get_task_of_day(master)
        
        response_data = {
            "name": user.name,
            "balance": balance,
            "status": "Мастер",
            "commission": commission,
            "first_level_invites": first_level_invites,
            "second_level_invites": second_level_invites,
            "task_of_day": task_of_day
        }
        return Response(response_data, status=status.HTTP_200_OK)

    

def get_task_of_day(master_profile):
    """
    Возвращает задание дня для мастера на основе:
      - Разницы между количеством завершённых (Completed) и закрытых (Closed) заявок (difference).
      - Количества приглашённых мастеров с подтверждённым депозитом (invited_with_deposit).
    Условия:
      - Для перехода с уровня 1 на 2: difference >= 10 и invited_with_deposit >= 1.
      - Для перехода с уровня 2 на 3: difference >= 30 и invited_with_deposit >= 3.
    Функция возвращает рекомендацию в виде строки.
    """
    # Подсчитываем количество заявок
    completed_count = ServiceRequest.objects.filter(master=master_profile, status='Completed').count()
    closed_count = ServiceRequest.objects.filter(master=master_profile, status='Closed').count()
    difference = completed_count - closed_count

    # Количество приглашённых мастеров с депозитом
    invited_with_deposit = count_invited_masters_with_deposit(master_profile.user)
    current_level = master_profile.level

    if current_level == 3:
        return "Вы достигли максимального уровня. Поздравляем!"
    elif current_level == 1:
        # Для перехода на уровень 2: difference >= 10, invited_with_deposit >= 1
        needed_orders = max(0, 10 - difference)
        needed_invites = max(0, 1 - invited_with_deposit)
        tasks = []
        if needed_orders > 0:
            tasks.append(f"выполните ещё {needed_orders} заказ{'ов' if needed_orders != 1 else ''}")
        if needed_invites > 0:
            tasks.append(f"пригласите ещё {needed_invites} мастера")
        if tasks:
            return "Задача дня: " + " и ".join(tasks) + " для перехода на уровень 2."
        else:
            return "Вы готовы перейти на уровень 2! Поздравляем!"
    elif current_level == 2:
        # Для перехода на уровень 3: difference >= 30, invited_with_deposit >= 3
        needed_orders = max(0, 30 - difference)
        needed_invites = max(0, 3 - invited_with_deposit)
        tasks = []
        if needed_orders > 0:
            tasks.append(f"выполните ещё {needed_orders} заказ{'ов' if needed_orders != 1 else ''}")
        if needed_invites > 0:
            tasks.append(f"пригласите ещё {needed_invites} мастера")
        if tasks:
            return "Задача дня: " + " и ".join(tasks) + " для перехода на уровень 3."
        else:
            return "Вы готовы перейти на уровень 3! Поздравляем!"
