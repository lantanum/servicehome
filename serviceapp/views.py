from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import UserRegistrationSerializer, ServiceRequestCreateSerializer, RequestHistorySerializer
from .models import ServiceRequest

class UserRegistrationView(APIView):
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