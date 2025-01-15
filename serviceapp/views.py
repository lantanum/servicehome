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
from .models import EquipmentType, ServiceRequest, ServiceType, User

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
                        "messages": openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Items(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    "message_text": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="Многострочный текст заявки"
                                    ),
                                    "finish_button_text": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="Текст кнопки для завершения заявки"
                                    ),
                                }
                            ),
                            description="Список объектов с данными по заявкам"
                        )
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

        # Проверка, что пользователь является мастером
        if user.role != 'Master':
            return Response({"detail": "Доступно только для пользователей с ролью 'Master'."}, 
                            status=status.HTTP_403_FORBIDDEN)

        # Получение мастера, связанного с пользователем
        try:
            master = user.master_profile  # или user.master
        except AttributeError:
            return Response({"detail": "Мастер не найден для данного пользователя."},
                            status=status.HTTP_404_NOT_FOUND)

        # Получение активных заявок (со статусом 'In Progress')
        active_requests = ServiceRequest.objects.filter(
            master=master, 
            status='In Progress'
        ).order_by('-created_at')

        # Если заявок нет, возвращаем одно сообщение
        if not active_requests.exists():
            return Response(
                {"messages": [{"message_text": "🥳Нет активных заявок!", "finish_button_text": ""}]},
                status=status.HTTP_200_OK
            )

        # Формируем список объектов с полями message_text и finish_button_text
        messages = []
        for req in active_requests:
            date_str = req.created_at.strftime('%d.%m.%Y') if req.created_at else ""
            
            # Многострочный текст заявки
            message_text = (
                f"Заявка {req.id}\n"
                f"Дата заявки: {date_str}\n"
                f"Город: {req.city_name or ''}\n"
                f"Адрес: {req.address or ''}\n"
                "🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️\n"
                f"Имя: {req.client.name}\n"
                f"Тел.: {req.client.phone}\n"
                "🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️\n"
                f"Тип оборудования: {req.equipment_type or ''}\n"
                f"Марка: {req.equipment_brand or ''}\n"
                f"Модель: {req.equipment_model or ''}\n"
                f"Комментарий: {req.description or ''}\n"
                "🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️🔸️\n"
                "Бесплатный выезд и диагностика* - Бесплатный выезд и диагностика только при оказании ремонта. "
                "ВНИМАНИЕ! - В случае отказа от ремонта - Диагностика и выезд платные (Цену формирует мастер)."
            )

            # Текст кнопки — "Сообщить о завершении {req.id}"
            finish_button_text = f"Сообщить о завершении {req.id}"

            messages.append({
                "message_text": message_text,
                "finish_button_text": finish_button_text
            })

        return Response({"messages": messages}, status=status.HTTP_200_OK)


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
        active_requests_count = ServiceRequest.objects.filter(master=master, status='in_progress').count()

        # Проверка баланса: если он отрицательный, возвращаем 0, иначе 1
        balance = 0 if balance < 0 else 1

        # Формируем ответ
        return Response({
            "balance": balance,
            "active_requests_count": active_requests_count
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
