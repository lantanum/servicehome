from datetime import timezone
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
from serviceapp.utils import STATUS_MAPPING, parse_nested_form_data
from .serializers import (
    AmoCRMWebhookSerializer,
    EquipmentTypeSerializer,
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
from .models import EquipmentType, Master, ReferralLink, ServiceRequest, ServiceType, Settings, Transaction, User

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
            user = User.objects.get(telegram_id=telegram_id)
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
                finish_button_text = f"Сообщить о завершении {req.id}"

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
                master_user = User.objects.select_for_update().get(telegram_id=telegram_id)
                master = master_user.master_profile

                # (1) Проверка баланса
                if master_user.balance < 0:
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
                    f"<b>ID</b> = {master_user.id}"
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
            "level": "1",          # заглушка
            "referral_count": total_referrals,
            "referral_count_1_line": count_1_line,
            "referral_count_2_line": count_2_line
        }

        return Response(response_data, status=status.HTTP_200_OK)




class ServiceEquipmentTypesView(APIView):
    """
    API-эндпоинт для получения списка типов сервисов и типов оборудования.
    """
    @swagger_auto_schema(
        operation_description="Получение списка типов сервисов и типов оборудования.",
        responses={
            200: openapi.Response(
                description="Список типов сервисов и оборудования",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "service_types": openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Items(type=openapi.TYPE_OBJECT, properties={
                                "id": openapi.Schema(type=openapi.TYPE_INTEGER, description="ID типа сервиса"),
                                "name": openapi.Schema(type=openapi.TYPE_STRING, description="Название типа сервиса")
                            })
                        ),
                        "equipment_types": openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Items(type=openapi.TYPE_OBJECT, properties={
                                "id": openapi.Schema(type=openapi.TYPE_INTEGER, description="ID типа оборудования"),
                                "name": openapi.Schema(type=openapi.TYPE_STRING, description="Название типа оборудования")
                            })
                        )
                    }
                )
            )
        }
    )
    def post(self, request):
        service_types = ServiceType.objects.all()
        equipment_types = EquipmentType.objects.all()
        
        service_serializer = ServiceTypeSerializer(service_types, many=True)
        equipment_serializer = EquipmentTypeSerializer(equipment_types, many=True)
        
        return Response({
            "service_types": service_serializer.data,
            "equipment_types": equipment_serializer.data
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


class AmoCRMWebhookView(APIView):
    """
    API-эндпоинт для приема вебхуков от AmoCRM о статусах лидов.
    """
    def post(self, request):
        try:
            # 1) Логируем и парсим данные
            raw_data = request.body.decode('utf-8')
            logger.debug(f"Incoming AmoCRM webhook raw data: {raw_data}")
        except Exception as e:
            logger.error(f"Error decoding request body: {e}")
            return Response({"detail": "Invalid request body."}, status=status.HTTP_400_BAD_REQUEST)

        nested_data = parse_nested_form_data(request.POST)
        logger.debug(f"Parsed AmoCRM webhook data: {nested_data}")

        serializer = AmoCRMWebhookSerializer(data=nested_data)
        if not serializer.is_valid():
            logger.warning(f"Invalid AmoCRM webhook data: {serializer.errors}")
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        embedded = serializer.validated_data.get('leads', {})
        status_changes = embedded.get('status', [])

        for lead in status_changes:
            try:
                lead_id = lead.get('id')
                new_status_id = lead.get('status_id')
                operator_comment = lead.get('748437', "")
                deal_success = lead.get('748715', "")

                with transaction.atomic():
                    service_request = ServiceRequest.objects.select_for_update().get(
                        amo_crm_lead_id=lead_id
                    )

                    # Сохраняем комментарий оператора
                    service_request.crm_operator_comment = operator_comment
                    service_request.deal_success = deal_success
                    service_request.save()

                    # Ищем статус-строку
                    status_name = None
                    for k, v in STATUS_MAPPING.items():
                        if v == new_status_id:
                            status_name = k
                            break

                    if not status_name:
                        logger.warning(
                            f"No matching status found in STATUS_MAPPING for status_id={new_status_id}"
                        )
                        continue

                    if status_name in ['AwaitingClosure', 'Closed', 'Completed']:
                        previous_status = service_request.status
                        service_request.status = status_name
                        service_request.amo_status_code = new_status_id
                        service_request.save()

                        logger.info(f"ServiceRequest {service_request.id}: status updated "
                                    f"from {previous_status} to '{status_name}' "
                                    f"(amoCRM ID={new_status_id}).")

                        if status_name == 'AwaitingClosure':
                            if service_request.master and service_request.master.user.telegram_id:
                                telegram_id_master = service_request.master.user.telegram_id
                                payload = {
                                    "telegram_id": telegram_id_master,
                                    "request_id": str(lead_id)
                                }
                                try:
                                    response_sambot = requests.post(
                                        'https://sambot.ru/reactions/2939774/start',
                                        json=payload,
                                        timeout=10
                                    )
                                    if response_sambot.status_code != 200:
                                        logger.error(
                                            f"Failed to send data to sambot (AwaitingClosure) for Request {service_request.id}. "
                                            f"Status code: {response_sambot.status_code}, Response: {response_sambot.text}"
                                        )
                                except Exception as ex:
                                    logger.error(f"Error sending data to sambot: {ex}")

                        elif status_name == 'Completed':
                            handle_completed_deal(
                                service_request=service_request,
                                operator_comment=operator_comment,
                                previous_status=previous_status,
                                lead_id=lead_id
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

        return Response({"detail": "Webhook processed."}, status=status.HTTP_200_OK)



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
    masters_round_1 = find_suitable_masters(service_request, round_num=1)
    logger.info(f"[ServiceRequest {service_request.id}] Найдено {len(masters_round_1)} мастеров для 1-го круга.")
    send_request_to_sambot(service_request, masters_round_1)

    # 2-й круг (через 10 минут)
    threading.Timer(60, send_request_to_sambot_with_logging, [service_request, 2]).start()

    # 3-й круг (через 20 минут)
    threading.Timer(120, send_request_to_sambot_with_logging, [service_request, 3]).start()

def send_request_to_sambot_with_logging(service_request, round_num):
    """
    Функция-обертка для логирования перед отправкой запроса.
    """
    logger.info(f"[ServiceRequest {service_request.id}] Запуск {round_num}-го круга рассылки.")
    masters = find_suitable_masters(service_request, round_num)
    logger.info(f"[ServiceRequest {service_request.id}] Найдено {len(masters)} мастеров для {round_num}-го круга.")
    send_request_to_sambot(service_request, masters)

def send_request_to_sambot(service_request, masters_telegram_ids):
    """
    Отправляет данные на Sambot.
    """
    if not masters_telegram_ids:
        logger.info(f"[ServiceRequest {service_request.id}] Нет мастеров для отправки в этом круге.")
        return

    # Генерация сообщений
    result = generate_free_status_data(service_request)

    payload = {
        "message_for_masters": result["message_for_masters"],
        "message_for_admin": result["message_for_admin"],
        "finish_button_text": result["finish_button_text"],
        "masters_telegram_ids": masters_telegram_ids
    }

    try:
        response = requests.post(
            'https://sambot.ru/reactions/2890052/start',
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

def find_suitable_masters(service_request, round_num):
    """
    Подбирает мастеров в зависимости от круга рассылки.
    """
    city_name = service_request.city_name.lower()
    equipment_type = (service_request.equipment_type or "").lower()

    masters = Master.objects.select_related('user').all()
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

    # Поиск мастеров по критериям
    masters_telegram_ids = find_suitable_masters(city_name, service_request.equipment_type)

    return {
        "message_for_masters": message_for_masters,
        "message_for_admin": message_for_admin,
        "finish_button_text": finish_button_text,
        "masters_telegram_ids": masters_telegram_ids
    }


def handle_completed_deal(service_request, operator_comment, previous_status, lead_id):
    """
    Обработка сделки со статусом 'Completed':
    1) Считаем комиссию
    2) Списываем комиссию
    3) Отправляем POST на sambot
    4) Пересчитываем уровень мастера (повышение / понижение)
    """
    from decimal import Decimal

    # Сумма сделки
    deal_amount = service_request.price or Decimal('0.00')

    # Получаем Master (если нет мастера - пропускаем)
    master_profile = service_request.master
    if not master_profile:
        logger.warning("ServiceRequest %s: no master assigned, skipping commission", service_request.id)
        return

    # Текущий уровень
    master_level = master_profile.level

    # 1) Комиссия из Settings
    settings_obj = Settings.objects.first()
    if not settings_obj:
        logger.warning("No Settings found! Commission = 0 by default.")
        commission_percentage = Decimal('0.0')
    else:
        if master_level == 1:
            commission_percentage = settings_obj.commission_level1
        elif master_level == 2:
            commission_percentage = settings_obj.commission_level2
        elif master_level == 3:
            commission_percentage = settings_obj.commission_level3
        else:
            commission_percentage = Decimal('0.0')

    # 2) Считаем комиссию
    from decimal import Decimal
    commission_amount = deal_amount * commission_percentage / Decimal('100')

    # 3) Списываем с баланса (профиль мастера → user)
    if master_profile.user:
        master_profile.user.balance -= commission_amount
        master_profile.user.save()

    # 4) Отправляем POST на sambot
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
            'https://sambot.ru/reactions/2939784/start',
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

    # 5) Пересчитываем уровень мастера после сделки
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
            user = User.objects.get(telegram_id=telegram_id)
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
    API-эндпоинт для того, чтобы мастер (или бот) мог завершить заявку,
    переведя её в статус "Контроль качества".
    """

    @swagger_auto_schema(
        operation_description="Закрытие заявки. Переводит заявку в статус 'Контроль качества' и обновляет данные в AmoCRM.",
        request_body=openapi.Schema(
            type=openapi.TYPE_OBJECT,
            properties={
                'request_id': openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки"),
                'finalAnsw1': openapi.Schema(type=openapi.TYPE_STRING, description="Какие работы были выполнены"),
                'finalAnsw2': openapi.Schema(type=openapi.TYPE_STRING, description="Гарантия"),
                'finalAnsw3': openapi.Schema(type=openapi.TYPE_STRING, description="Итоговая цена (число)"),
                'finalAnsw4': openapi.Schema(type=openapi.TYPE_STRING, description="Сумма, потраченная на запчасти"),
            },
            required=['request_id']
        ),
        responses={
            200: openapi.Response(
                description="Заявка переведена в статус 'Контроль качества'",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING, description='Сообщение об успешном завершении')
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
                description="Заявка не найдена",
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
        data = request.data

        # Считываем все входные данные как текст (строки)
        finalAnsw1 = data.get('finalAnsw1', "")      # какие работы были выполнены
        finalAnsw2 = data.get('finalAnsw2', "")      # гарантия
        finalAnsw3 = data.get('finalAnsw3', "")      # итоговая цена
        finalAnsw4 = data.get('finalAnsw4', "")      # сумма, потраченная на запчасти
        finish_button_text = data.get('finish_button_text', "")  # "Сообщить о завершении 123123"

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
                        "price": int(price_value),   # !!! приводим к int
                        "custom_fields_values": custom_fields
                    }
                )

            return JsonResponse(
                {
                    "detail": f"Заявка {request_id} успешно переведена в статус 'Контроль качества'.",
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
            user = User.objects.get(telegram_id=telegram_id)
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
            user = User.objects.get(telegram_id=telegram_id)
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
            user = User.objects.get(telegram_id=telegram_id)
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

            master.balance += tx.amount
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
