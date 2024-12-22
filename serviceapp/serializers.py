from rest_framework import serializers
from .models import User, Master, ServiceRequest

class UserRegistrationSerializer(serializers.Serializer):
    # Обязательные и необязательные поля для регистрации пользователя
    name = serializers.CharField(required=True)
    phone = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    telegram_id = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    telegram_login = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    role = serializers.ChoiceField(choices=User.ROLE_CHOICES)
    city_name = serializers.CharField(required=False, allow_blank=True, help_text="Название города")
    service_name = serializers.CharField(required=False, allow_blank=True, help_text="Название услуги")
    address = serializers.CharField(required=False, allow_blank=True, help_text="Адрес работы мастера")

    def validate(self, attrs):
        role = attrs.get('role')
        # Проверяем, что если регистрируется мастер или клиент, то все необходимые данные переданы
        errors = {}
        if role == 'Master':
            if not attrs.get('city_name'):
                errors['city_name'] = "Необходимо указать название города для регистрации мастера."
            if not attrs.get('service_name'):
                errors['service_name'] = "Необходимо указать название услуги для регистрации мастера."
            if not attrs.get('address'):
                errors['address'] = "Необходимо указать адрес работы мастера."
        elif role == 'Client':
            if not attrs.get('city_name'):
                errors['city_name'] = "Необходимо указать название города для регистрации клиента."
        if errors:
            raise serializers.ValidationError(errors)
        return attrs

    def create(self, validated_data):
        role = validated_data.get('role', 'Client')
        city_name = validated_data.pop('city_name', None)
        service_name = validated_data.pop('service_name', None)
        address = validated_data.pop('address', None)

        # Создаём пользователя
        user = User.objects.create(
            name=validated_data['name'],
            phone=validated_data.get('phone'),
            telegram_id=validated_data.get('telegram_id'),
            telegram_login=validated_data.get('telegram_login'),
            role=role,
            city_name=city_name  # Устанавливаем для клиентов
        )

        # Если роль - мастер, то создаём профиль мастера
        if role == 'Master':
            Master.objects.create(
                user=user,
                city_name=city_name,
                service_name=service_name,
                address=address
            )
            # В данном случае мы сохраняем service_name в заявки, поэтому здесь дополнительная логика не нужна
            # Если необходимо, можно добавить сохранение service_name в отдельную модель или другое поле

        return user

class ServiceRequestCreateSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID клиента")
    service_name = serializers.CharField(required=True, help_text="Название услуги")
    city_name = serializers.CharField(required=True, help_text="Название города")
    address = serializers.CharField(required=True, allow_blank=False, help_text="Адрес")
    description = serializers.CharField(required=False, allow_blank=True, help_text="Описание заявки")

    def validate_telegram_id(self, value):
        # Проверяем, существует ли пользователь с таким telegram_id и является ли он клиентом
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")

        if user.role != 'Client':
            raise serializers.ValidationError("Только пользователи с ролью 'Client' могут создавать заявки.")
        return value

    def create(self, validated_data):
        telegram_id = validated_data['telegram_id']
        service_name = validated_data['service_name']
        city_name = validated_data['city_name']
        address = validated_data['address']
        description = validated_data.get('description', '')

        # Получаем пользователя-клиента
        client = User.objects.get(telegram_id=telegram_id, role='Client')

        # Создаём заявку
        service_request = ServiceRequest.objects.create(
            client=client,
            service_name=service_name,
            city_name=city_name,
            address=address,
            description=description,
            status='Open'
        )
        return service_request

class RequestHistorySerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True)

class MasterActiveRequestsSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID мастера")

class AssignRequestSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID мастера")
    request_id = serializers.IntegerField(required=True, help_text="ID заявки")
    
    def validate_telegram_id(self, value):
        # Проверяем, существует ли пользователь с таким telegram_id и является ли он мастером
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")
        
        if user.role != 'Master':
            raise serializers.ValidationError("Только пользователи с ролью 'Master' могут брать заявки в работу.")
        
        # Проверяем, что у пользователя есть связанный объект Master
        if not hasattr(user, 'master'):
            raise serializers.ValidationError("Мастерская информация не найдена для данного пользователя.")
        
        return value

    def validate_request_id(self, value):
        # Проверяем, существует ли заявка с таким ID и находится ли она в статусе 'Open'
        try:
            service_request = ServiceRequest.objects.get(id=value)
        except ServiceRequest.DoesNotExist:
            raise serializers.ValidationError("Заявка с указанным ID не найдена.")
        
        if service_request.status != 'Open':
            raise serializers.ValidationError("Заявка уже назначена или не может быть взята в работу.")
        
        return value

    
class CloseRequestSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID мастера")
    request_id = serializers.IntegerField(required=True, help_text="ID заявки")
    client_rating = serializers.DecimalField(
        max_digits=5, decimal_places=2, required=True, help_text="Оценка клиента"
    )
    # Дополнительные поля, если необходимо
    # comment = serializers.CharField(required=False, allow_blank=True, help_text="Комментарий мастера")
    
    def validate_telegram_id(self, value):
        # Проверяем, существует ли пользователь с таким telegram_id и является ли он мастером
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")
        
        if user.role != 'Master':
            raise serializers.ValidationError("Только пользователи с ролью 'Master' могут закрывать заявки.")
        
        # Проверяем, что у пользователя есть связанный объект Master
        if not hasattr(user, 'master'):
            raise serializers.ValidationError("Мастерская информация не найдена для данного пользователя.")
        
        return value

    def validate_request_id(self, value):
        # Проверяем, существует ли заявка с таким ID и находится ли она в статусе 'In Progress'
        try:
            service_request = ServiceRequest.objects.get(id=value)
        except ServiceRequest.DoesNotExist:
            raise serializers.ValidationError("Заявка с указанным ID не найдена.")
        
        if service_request.status != 'In Progress':
            raise serializers.ValidationError("Заявка должна быть в статусе 'In Progress' для закрытия.")
        
        return value

    def validate_client_rating(self, value):
        if not (1.0 <= value <= 5.0):
            raise serializers.ValidationError("Оценка должна быть между 1.0 и 5.0.")
        return value

    def create(self, validated_data):
        telegram_id = validated_data['telegram_id']
        request_id = validated_data['request_id']
        client_rating = validated_data['client_rating']
        # comment = validated_data.get('comment', '')  # Если используется комментарий

        # Получаем пользователя-мастера и заявку
        master_user = User.objects.get(telegram_id=telegram_id)
        master = master_user.master
        service_request = ServiceRequest.objects.get(id=request_id)

        # Обновляем заявку
        service_request.master = master
        service_request.status = 'Completed'
        service_request.client_rating = client_rating
        service_request.completed_at = timezone.now()
        # service_request.comment = comment  # Если используется комментарий
        service_request.save()

        return service_request


class MinimalUserSerializer(serializers.ModelSerializer):
    """
    Сериализатор для минимальной информации о пользователе.
    Используется для отображения реферера и рефералов.
    """
    class Meta:
        model = User
        fields = ['id', 'name', 'telegram_login']


class UserProfileSerializer(serializers.ModelSerializer):
    """
    Сериализатор для профиля пользователя, включая рефералов.
    """
    referrer = serializers.SerializerMethodField()
    referees = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ['id', 'name', 'phone', 'role', 'created_at', 'referrer', 'referees']

    def get_referrer(self, obj):
        """
        Возвращает информацию о пользователе, который пригласил данного пользователя.
        """
        try:
            referral_link = obj.referral_links_received.first()
            if referral_link and referral_link.referrer_user:
                return MinimalUserSerializer(referral_link.referrer_user).data
            return None
        except ReferralLink.DoesNotExist:
            return None

    def get_referees(self, obj):
        """
        Возвращает список пользователей, которых пригласил данный пользователь.
        """
        referees = obj.referral_links_given.all()
        users = [link.referred_user for link in referees]
        return MinimalUserSerializer(users, many=True).data


class UserProfileRequestSerializer(serializers.Serializer):
    """
    Сериализатор для запроса профиля пользователя по telegram_id.
    """
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID пользователя")