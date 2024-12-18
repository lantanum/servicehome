from rest_framework import serializers
from .models import User, Master, City, Service, MasterService

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
