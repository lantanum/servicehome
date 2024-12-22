# users_app/serializers.py

from rest_framework import serializers
from django.utils import timezone
from django.db import transaction
from .models import User, Master, ServiceRequest, ReferralLink  # Предполагается, что ReferralLink существует


# Минимальный сериализатор для отображения информации о пользователе
class MinimalUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'name', 'telegram_login']


# Сериализатор для регистрации пользователя или мастера
class UserRegistrationSerializer(serializers.ModelSerializer):
    service_name = serializers.CharField(
        required=False, allow_blank=True, help_text="Название услуги"
    )
    address = serializers.CharField(
        required=False, allow_blank=True, help_text="Адрес работы мастера"
    )

    class Meta:
        model = User
        fields = [
            'id',
            'name',
            'phone',
            'telegram_id',
            'telegram_login',
            'role',
            'city_name',
            'service_name',
            'address',
        ]

    def validate(self, attrs):
        role = attrs.get('role')
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
        role = validated_data.pop('role', 'Client')
        city_name = validated_data.pop('city_name', None)
        service_name = validated_data.pop('service_name', None)
        address = validated_data.pop('address', None)

        user = User.objects.create(
            role=role,
            city_name=city_name,
            **validated_data
        )

        if role == 'Master':
            Master.objects.create(
                user=user,
                city_name=city_name,
                service_name=service_name,
                address=address
            )

        return user


# Сериализатор для создания заявки
class ServiceRequestCreateSerializer(serializers.ModelSerializer):
    telegram_id = serializers.CharField(
        required=True, help_text="Telegram ID клиента"
    )

    class Meta:
        model = ServiceRequest
        fields = [
            'id',
            'telegram_id',
            'service_name',
            'city_name',
            'address',
            'description',
        ]

    def validate_telegram_id(self, value):
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")

        if user.role != 'Client':
            raise serializers.ValidationError("Только пользователи с ролью 'Client' могут создавать заявки.")
        return value

    def create(self, validated_data):
        telegram_id = validated_data.pop('telegram_id')
        service_name = validated_data.get('service_name')
        city_name = validated_data.get('city_name')
        address = validated_data.get('address')
        description = validated_data.get('description', '')

        client = User.objects.get(telegram_id=telegram_id, role='Client')

        service_request = ServiceRequest.objects.create(
            client=client,
            service_name=service_name,
            city_name=city_name,
            address=address,
            description=description,
            status='Open'
        )
        return service_request


# Сериализатор для запроса истории заявок
class RequestHistorySerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID клиента")


# Сериализатор для запроса активных заявок мастера
class MasterActiveRequestsSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID мастера")


# Сериализатор для назначения заявки мастеру
class AssignRequestSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID мастера")
    request_id = serializers.IntegerField(required=True, help_text="ID заявки")

    def validate_telegram_id(self, value):
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")

        if user.role != 'Master':
            raise serializers.ValidationError("Только пользователи с ролью 'Master' могут брать заявки в работу.")

        if not hasattr(user, 'master'):
            raise serializers.ValidationError("Мастерская информация не найдена для данного пользователя.")

        return value

    def validate_request_id(self, value):
        try:
            service_request = ServiceRequest.objects.get(id=value)
        except ServiceRequest.DoesNotExist:
            raise serializers.ValidationError("Заявка с указанным ID не найдена.")

        if service_request.status != 'Open':
            raise serializers.ValidationError("Заявка уже назначена или не может быть взята в работу.")

        return value


# Сериализатор для закрытия заявки мастером
class CloseRequestSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID мастера")
    request_id = serializers.IntegerField(required=True, help_text="ID заявки")
    # comment = serializers.CharField(required=False, allow_blank=True, help_text="Комментарий мастера")

    def validate_telegram_id(self, value):
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь с указанным telegram_id не найден.")

        if user.role != 'Master':
            raise serializers.ValidationError("Только пользователи с ролью 'Master' могут закрывать заявки.")

        if not hasattr(user, 'master'):
            raise serializers.ValidationError("Мастерская информация не найдена для данного пользователя.")

        return value

    def validate_request_id(self, value):
        try:
            service_request = ServiceRequest.objects.get(id=value)
        except ServiceRequest.DoesNotExist:
            raise serializers.ValidationError("Заявка с указанным ID не найдена.")

        if service_request.status != 'In Progress':
            raise serializers.ValidationError("Заявка должна быть в статусе 'In Progress' для закрытия.")

        return value

    def validate(self, attrs):
        # Дополнительная валидация, если необходимо
        return attrs

    def create(self, validated_data):
        telegram_id = validated_data['telegram_id']
        request_id = validated_data['request_id']
        # comment = validated_data.get('comment', '')  # Если используется комментарий

        master_user = User.objects.get(telegram_id=telegram_id)
        master = master_user.master
        service_request = ServiceRequest.objects.get(id=request_id)

        with transaction.atomic():
            service_request.status = 'Completed'
            service_request.completed_at = timezone.now()
            # service_request.comment = comment  # Если используется комментарий
            service_request.save()

        return service_request


# Сериализатор для профиля пользователя
class UserProfileSerializer(serializers.ModelSerializer):
    master = serializers.SerializerMethodField()
    referrer = serializers.SerializerMethodField()
    referees = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            'id',
            'name',
            'phone',
            'role',
            'created_at',
            'referrer',
            'referees',
            'master',
        ]

    def get_master(self, obj):
        try:
            master = obj.master
            return MasterSerializer(master).data
        except Master.DoesNotExist:
            return None

    def get_referrer(self, obj):
        try:
            referral_link = ReferralLink.objects.get(referred_user=obj)
            return MinimalUserSerializer(referral_link.referrer_user).data
        except ReferralLink.DoesNotExist:
            return None

    def get_referees(self, obj):
        referees = ReferralLink.objects.filter(referrer_user=obj)
        users = [link.referred_user for link in referees]
        return MinimalUserSerializer(users, many=True).data


# Сериализатор для запроса профиля пользователя
class UserProfileRequestSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID пользователя")


# Сериализатор для отображения мастера
class MasterSerializer(serializers.ModelSerializer):
    user = MinimalUserSerializer(read_only=True)

    class Meta:
        model = Master
        fields = [
            'id',
            'user',
            'address',
            'rating',
            'balance',
            'city_name',
            'service_name',
        ]


# Сериализатор для отображения заявки
class ServiceRequestSerializer(serializers.ModelSerializer):
    client = MinimalUserSerializer(read_only=True)
    master = MasterSerializer(read_only=True)

    class Meta:
        model = ServiceRequest
        fields = [
            'id',
            'client',
            'master',
            'service_name',
            'city_name',
            'status',
            'price',
            'client_rating',
            'address',
            'cancellation_reason',
            'created_at',
            'completed_at',
            'description',
        ]
