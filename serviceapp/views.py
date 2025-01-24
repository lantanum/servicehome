from datetime import timezone
from decimal import Decimal
import logging
import re
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
from .models import EquipmentType, Master, ServiceRequest, ServiceType, Transaction, User

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
                "request_id": service_request.id
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
                        # аналогично для request_2..request_10
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

        # Получаем заявки 'In Progress', максимум 10
        active_requests = ServiceRequest.objects.filter(
            master=master, 
            status__in=['In Progress', 'AwaitingClosure']
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

            date_str = req.created_at.strftime('%d.%m.%Y') if req.created_at else ""

            # Формируем HTML-строку с <b>...</b>
            message_text = (
                f"<b>Заявка</b> {req.id}\n"
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
                'request_id': openapi.Schema(type=openapi.TYPE_STRING, description="ID заявки")
            },
            required=['telegram_id', 'request_id']
        ),
        responses={
            200: openapi.Response(
                description="Заявка успешно взята в работу",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING, description='Сообщение об успешном присвоении заявки')
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные или заявка уже назначена",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
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
        data = request.data
        telegram_id = data.get('telegram_id')
        request_id = data.get('request_id')

        if not telegram_id or not request_id:
            return JsonResponse({'error': 'telegram_id and request_id are required'}, status=400)

        try:
            with transaction.atomic():
                master_user = User.objects.select_for_update().get(telegram_id=telegram_id)
                master = master_user.master_profile

                service_request = ServiceRequest.objects.select_for_update().get(id=request_id)

                

                # Запоминаем исходный статус до изменения
                original_status = service_request.status

                if original_status == 'Free':
                    # 1) Выполняем привязку заявки к мастеру и переводим в In Progress
                    service_request.master = master
                    service_request.status = 'In Progress'
                    service_request.start_date = timezone.now() 
                    service_request.save()

                    # 2) Обновляем в amoCRM (статус и контакт)
                    lead_id = service_request.amo_crm_lead_id
                    master_contact_id = master_user.amo_crm_contact_id

                    if not lead_id or not master_contact_id:
                        return JsonResponse(
                            {'error': 'AmoCRM IDs for request or master are missing'}, 
                            status=400
                        )

                    amocrm_client = AmoCRMClient()

                    category_service = master.service_name or ""
                    equipment_type_value = master.equipment_type_name or ""
                    # И т.д. для остальных полей...

                    # Вызов обновления лида с добавлением кастомных полей
                    amocrm_client.update_lead(
                        lead_id,
                        {
                            "status_id": STATUS_MAPPING["In Progress"],
                            "custom_fields_values": [
                                { 
                                    "field_id": 748205,  # категория услуг мастера
                                    "values": [{"value": category_service}]
                                },
                                {
                                    "field_id": 748321,  # тип оборудования мастера
                                    "values": [{"value": equipment_type_value}]
                                },
                                {
                                    "field_id": 748327,  # кол-во рефералов
                                    "values": [{"value": "подходящее значение"}]
                                },
                                {
                                    "field_id": 748213,  # процент затрат с работ мастера
                                    "values": [{"value": "подходящее значение"}]
                                },
                                {
                                    "field_id": 748329,  # баланс мастера
                                    "values": [{"value": str(master.balance)}]

                                }
                            ]
                        }
                    )


                    # Прикрепляем контакт
                    amocrm_client.attach_contact_to_lead(lead_id, master_contact_id)

                    # -- Формируем расширенный ответ: несмотря на то,
                    #    что в базе уже стал "In Progress",
                    #    отдать в JSON именно "Free" + нужные поля.

                    status_id = STATUS_MAPPING.get('Free', None)
                    created_date_str = (service_request.created_at.strftime('%d.%m.%Y')
                                        if service_request.created_at else None)

                    # Город отдельно
                    city_name = service_request.city_name or ""

                    # Адрес: только первое слово
                    raw_address = service_request.address or ""
                    address_parts = raw_address.strip().split()
                    short_address = address_parts[0] if address_parts else ""

                    response_data = {
                        "status_id": status_id,         # числовой ID статуса 'Free'
                        "request_id": service_request.id,
                        "request_date": created_date_str,
                        "city_name": city_name,
                        "address": raw_address,
                        "short_address": short_address,
                        "client_telegram_id": service_request.client.telegram_id,
                        "client_name": service_request.client.name,
                        "client_phone": service_request.client.phone,
                        "equipment_type": service_request.equipment_type,
                        "equipment_brand": service_request.equipment_brand,
                        "equipment_model": service_request.equipment_model,
                        "comment": service_request.description,
                    }
                    return JsonResponse(response_data, status=200)

                elif original_status == 'In Progress':
                    # Если заявка уже 'In Progress', ничего не меняем,
                    # просто отвечаем status_id
                    status_id = STATUS_MAPPING.get('In Progress', None)
                    return JsonResponse({"status_id": status_id}, status=200)

                else:
                    # Если нужно, можно выбросить ошибку, 
                    # или обрабатывать "Open"/"Cancelled" и т. д. особым образом
                    return JsonResponse(
                        {"detail": f"Заявка в статусе {original_status}, обработка не предусмотрена."},
                        status=400
                    )

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
                schema=UserProfileSerializer
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
        # Валидируем входные данные
        serializer = UserProfileRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        telegram_id = serializer.validated_data['telegram_id']
        
        # Пытаемся найти пользователя по telegram_id с предзагрузкой рефералов
        try:
            user = User.objects.select_related('master').prefetch_related('referral_links_received', 'referral_links_given').get(telegram_id=telegram_id)
        except User.DoesNotExist:
            return Response(
                {"detail": "Пользователь с указанным telegram_id не найден."},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Сериализуем данные профиля пользователя
        profile_serializer = UserProfileSerializer(user)
        return Response(profile_serializer.data, status=status.HTTP_200_OK)



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
        # 1) Логируем и парсим данные (как у вас уже есть)
        try:
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

        # 2) Обрабатываем все статусы, пришедшие в webhook
        for lead in status_changes:
            try:
                lead_id = lead.get('id')
                new_status_id = lead.get('status_id')

                with transaction.atomic():
                    service_request = ServiceRequest.objects.select_for_update().get(
                        amo_crm_lead_id=lead_id
                    )

                    # Сопоставляем числовой new_status_id со строковым ключом
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

                    # Теперь у нас есть статус в виде строки, например "AwaitingClosure" или "Closed".
                    # Логика обновления:
                    if status_name in ['AwaitingClosure', 'Closed', 'Completed']:
                        previous_status = service_request.status
                        service_request.status = status_name  # сохраняем в Django-модели (AwaitingClosure, Closed, etc.)
                        service_request.amo_status_code = new_status_id  # полезно хранить исходный int-статус в amo
                        service_request.save()

                        logger.info(f"ServiceRequest {service_request.id}: status updated "
                                    f"from {previous_status} to '{status_name}' "
                                    f"(amoCRM ID={new_status_id}).")
                        if status_name == 'AwaitingClosure':
                            # Предположим, что заявка должна иметь назначенного мастера
                            if service_request.master and service_request.master.user.telegram_id:
                                telegram_id_master = service_request.master.user.telegram_id
                                # Отправляем POST-запрос на sambot
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
                            # Отправляем нужные поля на https://sambot.ru/reactions/2939784/start
                            # айди сделки (lead_id), айди мастера, сообщение о штрафе (пустая строка), 
                            # сумма сделки (service_request.price), прошлый статус
                            master_id = service_request.master.id if service_request.master else ""
                            deal_amount = str(service_request.price or "0")
                            penalty_message = ""  # Пустое поле

                            payload = {
                                "request_id": lead_id,
                                "telegram_id": master_id,
                                "penalty_message": penalty_message,
                                "request_amount": deal_amount,
                                "previous_status": previous_status
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


                    elif status_name == 'Free':
                        previous_status = service_request.status
                        service_request.status = 'Free'
                        service_request.amo_status_code = new_status_id
                        service_request.save()

                        logger.info(f"ServiceRequest {service_request.id}: status updated "
                                    f"from {previous_status} to 'Free'.")

                        # тут ваша логика отправки на внешний сервис
                        payload = {
                            "id": service_request.id,
                            "город_заявки": service_request.city_name,
                            "адрес": extract_street_name(service_request.address),
                            "дата_заявки": format_date(service_request.created_at),
                            "тип_оборудования": service_request.equipment_type,
                            "марка": service_request.equipment_brand,
                            "модель": service_request.equipment_model,
                            "комментарий": service_request.description or ""
                        }

                        external_response = requests.post(
                            'https://sambot.ru/reactions/2890052/start',
                            json=payload,
                            timeout=10
                        )
                        if external_response.status_code != 200:
                            logger.error(
                                f"Failed to send data to external service for ServiceRequest {service_request.id}. "
                                f"Status code: {external_response.status_code}, Response: {external_response.text}"
                            )
                    else:
                        logger.info(f"Ignoring status {status_name} (id={new_status_id}) for lead_id={lead_id}")

            except ServiceRequest.DoesNotExist:
                logger.error(f"ServiceRequest with amo_crm_lead_id={lead_id} does not exist.")
                continue
            except Exception as e:
                logger.exception(f"Error processing lead_id={lead_id}: {e}")
                continue

        return Response({"detail": "Webhook processed."}, status=status.HTTP_200_OK)

    


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
        active_requests_count = ServiceRequest.objects.filter(master=master, status='In Progress').count()

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
                service_request = ServiceRequest.objects.select_for_update().get(id=request_id)

                price_value = Decimal(finalAnsw3) if finalAnsw3 else Decimal("0")
                spare_parts_value = Decimal(finalAnsw4) if finalAnsw4 else Decimal("0")

                service_request.comment_after_finish = finalAnsw1
                service_request.warranty = finalAnsw2
                service_request.price = price_value
                service_request.spare_parts_spent = spare_parts_value
                service_request.status = 'QualityControl'
                service_request.end_date = timezone.now() 
                service_request.save()

                commission_value = (price_value * Decimal("0.3"))  # 10%

                master = service_request.master
                if master:
                    old_balance = master.balance
                    new_balance = old_balance - commission_value
                    master.balance = new_balance
                    master.save()
                else:
                    new_balance = Decimal("0")
                    master = None

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


            commission_str = str(int(commission_value))      # Преобразовали в int => без десятичной точки
            balance_str = str(int(new_balance))              # Аналогично

            rating_str = "5"  # заглушка без точки, например "5"
            ref_count_level1_str = "3"
            ref_count_level2_str = "1"
            master_level_str = "2"

            return JsonResponse(
                {
                    "detail": f"Заявка {request_id} успешно переведена в статус 'Контроль качества'.",
                    "comission": commission_str,
                    "balance": balance_str,
                    "rating": rating_str,
                    "ref_count_level1": ref_count_level1_str,
                    "ref_count_level2": ref_count_level2_str,
                    "master_level": master_level_str
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
                f"<b>Заявка </b> {req.id}\n"
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

            take_button_text = f"Взять заявку {req.id}"

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
    2. Подтверждает транзакцию пополнения (по transaction_id),
       переводит её в статус 'Confirmed' и увеличивает баланс мастера.
    """

    @swagger_auto_schema(
        operation_description="Подтверждает ранее созданную транзакцию (transaction_id), меняет её статус на 'Confirmed' и увеличивает баланс мастера.",
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
                description="Транзакция подтверждена, баланс мастера обновлён.",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        "detail": openapi.Schema(type=openapi.TYPE_STRING),
                        "new_balance": openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            400: openapi.Response(
                description="Некорректные данные (transaction_id не указан / неверный формат / транзакция уже подтверждена)",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'detail': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            404: openapi.Response(
                description="Транзакция не найдена / пользователь не является мастером",
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
            return Response(
                {"detail": "Поле 'transaction_id' обязательно."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Пробуем привести к int
        try:
            tx_id = int(tx_id)
        except ValueError:
            return Response(
                {"detail": "transaction_id должно быть целым числом."},
                status=status.HTTP_400_BAD_REQUEST
            )

        with transaction.atomic():
            # Находим транзакцию
            try:
                tx = Transaction.objects.select_for_update().get(id=tx_id)
            except Transaction.DoesNotExist:
                return Response(
                    {"detail": "Транзакция не найдена."},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Проверяем, что транзакция Pending
            if tx.status == 'Confirmed':
                return Response(
                    {"detail": "Транзакция уже подтверждена."},
                    status=status.HTTP_400_BAD_REQUEST
                )
            # Проверяем, что это Deposit
            if tx.transaction_type != 'Deposit':
                return Response(
                    {"detail": "Транзакция не является пополнением (Deposit)."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Переводим транзакцию в статус 'Confirmed'
            tx.status = 'Confirmed'
            tx.save()

            # Обновляем баланс мастера
            user = tx.user
            master = getattr(user, 'master_profile', None)
            if not master:
                return Response(
                    {"detail": "Пользователь не является мастером."},
                    status=status.HTTP_404_NOT_FOUND
                )

            master.balance += tx.amount
            master.save()

            new_balance_str = str(master.balance)

        return Response(
            {
                "detail": "Транзакция подтверждена, баланс успешно обновлён.",
                "new_balance": new_balance_str,
                "telegram_id": user.telegram_id
            },
            status=status.HTTP_200_OK
        )