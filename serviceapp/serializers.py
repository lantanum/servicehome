from rest_framework import serializers
from .models import User, Master, City, Service, MasterService, ServiceRequest

class UserRegistrationSerializer(serializers.Serializer):
    # Обязательные и необязательные поля для регистрации пользователя
    name = serializers.CharField(required=True)
    phone = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    telegram_id = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    telegram_login = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    role = serializers.ChoiceField(choices=User.ROLE_CHOICES)
    city_name = serializers.CharField(required=False, allow_blank=True)
    service_name = serializers.CharField(required=False, allow_blank=True)
    address = serializers.CharField(required=False, allow_blank=True)  # Адрес работы мастера

    def validate(self, attrs):
        role = attrs.get('role')
        # Проверяем, что если регистрируется мастер, то все необходимые данные переданы
        if role == 'Master':
            if not attrs.get('city_name'):
                raise serializers.ValidationError("Необходимо указать название города для регистрации мастера.")
            if not attrs.get('service_name'):
                raise serializers.ValidationError("Необходимо указать название услуги для регистрации мастера.")
            if not attrs.get('address'):
                raise serializers.ValidationError("Необходимо указать адрес работы мастера.")
        return attrs

    def create(self, validated_data):
        role = validated_data.get('role', 'Client')
        city_name = validated_data.pop('city_name', None)
        service_name = validated_data.pop('service_name', None)
        address = validated_data.pop('address', None)

        # Создаём запись пользователя
        user = User.objects.create(**validated_data)
        
        # Если роль - мастер, то создаём запись мастера и связываем его с городом и услугой
        if role == 'Master':
            city, _ = City.objects.get_or_create(name=city_name)
            master = Master.objects.create(user=user, city=city, address=address)
            service, _ = Service.objects.get_or_create(name=service_name)
            MasterService.objects.create(master=master, service=service)

        return user

class ServiceRequestCreateSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True)  # Определяем клиента по telegram_id
    service_id = serializers.IntegerField(required=True)
    city_id = serializers.IntegerField(required=True)
    address = serializers.CharField(required=True, allow_blank=False)
    description = serializers.CharField(required=False, allow_blank=True)

    def validate_telegram_id(self, value):
        # Проверяем, есть ли пользователь с таким telegram_id
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")

        # Проверяем, что этот пользователь - клиент
        if user.role != 'Client':
            raise serializers.ValidationError("Только пользователи с ролью 'Client' могут создавать заявки.")
        return value

    def validate_service_id(self, value):
        # Проверяем, что сервис существует
        if not Service.objects.filter(id=value).exists():
            raise serializers.ValidationError("Указанный сервис не найден.")
        return value

    def validate_city_id(self, value):
        # Проверяем, что город существует
        if not City.objects.filter(id=value).exists():
            raise serializers.ValidationError("Указанный город не найден.")
        return value

    def create(self, validated_data):
        telegram_id = validated_data['telegram_id']
        service_id = validated_data['service_id']
        city_id = validated_data['city_id']
        address = validated_data['address']
        description = validated_data.get('description', '')

        # Получаем пользователя по telegram_id
        user = User.objects.get(telegram_id=telegram_id, role='Client')

        # Создаём заявку
        service_request = ServiceRequest.objects.create(
            client=user,
            service_id=service_id,
            city_id=city_id,
            address=address,
            description=description,
            status='Open'  # По умолчанию заявка создаётся открытой
        )
        return service_request

class ServiceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Service
        fields = ['id', 'name']

class ServiceRequestSerializer(serializers.ModelSerializer):
    service = ServiceSerializer(read_only=True)  # Для удобства возвращаем информацию о сервисе
    
    class Meta:
        model = ServiceRequest
        fields = ['id', 'service', 'status', 'created_at', 'description']

class RequestHistorySerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True)