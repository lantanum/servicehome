import logging
from django.conf import settings
import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from serviceapp.utils import STATUS_MAPPING, parse_nested_form_data
from .serializers import (
    AmoCRMWebhookSerializer,
    EquipmentTypeSerializer,
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
                description="Успешный ответ с активными заявками",
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
            master = user.master
        except AttributeError:
            return Response({"detail": "Мастер не найден для данного пользователя."},
                            status=status.HTTP_404_NOT_FOUND)

        # Получение активных заявок (со статусом 'In Progress')
        active_requests = ServiceRequest.objects.filter(master=master, status='In Progress').order_by('-created_at')

        # Сериализация заявок
        requests_serializer = ServiceRequestSerializer(active_requests, many=True)
        return Response(requests_serializer.data, status=status.HTTP_200_OK)


class AssignRequestView(APIView):
    """
    API-эндпоинт для того, чтобы мастер мог взять заявку в работу по её ID.
    """
    @swagger_auto_schema(
        operation_description="Мастер берет заявку в работу по её ID.",
        request_body=AssignRequestSerializer,
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
        serializer = AssignRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        telegram_id = serializer.validated_data['telegram_id']
        request_id = serializer.validated_data['request_id']
        
        try:
            with transaction.atomic():
                # Получаем пользователя-мастера
                master_user = User.objects.select_for_update().get(telegram_id=telegram_id)
                master = master_user.master  # Предполагается, что связь OneToOneField
                
                # Получаем заявку
                service_request = ServiceRequest.objects.select_for_update().get(id=request_id)
                
                # Дополнительная проверка статуса (на случай, если статус изменился после валидации)
                if service_request.status != 'Open':
                    return Response(
                        {"detail": "Заявка уже назначена или не может быть взята в работу."},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                # Присваиваем заявку мастеру и обновляем статус
                service_request.master = master
                service_request.status = 'In Progress'
                service_request.save()
                
        except User.DoesNotExist:
            return Response({"detail": "Пользователь с указанным telegram_id не найден."},
                            status=status.HTTP_404_NOT_FOUND)
        except ServiceRequest.DoesNotExist:
            return Response({"detail": "Заявка с указанным ID не найдена."},
                            status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({"detail": "Произошла ошибка при присвоении заявки."},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        return Response({"detail": "Заявка успешно взята в работу."}, status=status.HTTP_200_OK)


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
    

class AmoCRMWebhookView(APIView):
    """
    API-эндпоинт для приема вебхуков от AmoCRM о статусах лидов.
    """

    def post(self, request):
        # Логирование сырого тела запроса
        try:
            raw_data = request.body.decode('utf-8')
            logger.debug(f"Incoming AmoCRM webhook raw data: {raw_data}")
        except Exception as e:
            logger.error(f"Error decoding request body: {e}")
            return Response({"detail": "Invalid request body."}, status=status.HTTP_400_BAD_REQUEST)

        # Преобразование плоских данных в вложенный словарь
        nested_data = parse_nested_form_data(request.POST)

        # Логирование преобразованных данных
        logger.debug(f"Parsed AmoCRM webhook data: {nested_data}")

        # Валидация преобразованных данных с помощью сериализатора
        serializer = AmoCRMWebhookSerializer(data=nested_data)
        if not serializer.is_valid():
            logger.warning(f"Invalid AmoCRM webhook data: {serializer.errors}")
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        # Извлечение изменений статусов лидов
        embedded = serializer.validated_data.get('leads', {})
        status_changes = embedded.get('status', [])

        for lead in status_changes:
            try:
                lead_id = lead.get('id')
                status_id = lead.get('status_id')

                # Проверяем, соответствует ли статус 'Free'
                if status_id == STATUS_MAPPING.get('Free'):
                    with transaction.atomic():
                        # Получаем заявку по amo_crm_lead_id
                        service_request = ServiceRequest.objects.select_for_update().get(amo_crm_lead_id=lead_id)

                        previous_status = service_request.status
                        service_request.status = 'Free'
                        service_request.save()
                        logger.info(f"ServiceRequest {service_request.id} updated from {previous_status} to 'Free'.")

                        # Подготавливаем данные для внешнего сервиса
                        payload = {
                            "город_заявки": service_request.city_name,
                            "адрес": service_request.address,
                            "дата_заявки": service_request.created_at.isoformat(),
                            "тип_оборудования": service_request.equipment_type,
                            "марка": service_request.equipment_brand,
                            "модель": service_request.equipment_model,
                            "комментарий": service_request.description or ""
                        }

                        # Отправляем POST-запрос на внешний сервис
                        external_response = requests.post(
                            'https://sambot.ru/reactions/2890052/start',
                            json=payload,
                            timeout=10  # Таймаут в секундах
                        )

                        if external_response.status_code != 200:
                            logger.error(
                                f"Failed to send data to external service for ServiceRequest {service_request.id}. "
                                f"Status code: {external_response.status_code}, Response: {external_response.text}"
                            )
                            # Опционально: Реализуйте повторные попытки или уведомления
            except ServiceRequest.DoesNotExist:
                logger.error(f"ServiceRequest with amo_crm_lead_id={lead_id} does not exist.")
                continue
            except Exception as e:
                logger.exception(f"Error processing lead_id={lead_id}: {e}")
                continue

        return Response({"detail": "Webhook processed."}, status=status.HTTP_200_OK)