from rest_framework import serializers
from django.utils import timezone
from django.db import transaction
from .models import EquipmentType, ServiceType, User, Master, ServiceRequest, ReferralLink  # Предполагается, что ReferralLink существует
from rest_framework import serializers
from django.conf import settings
from .models import User, Master
from .amocrm_client import AmoCRMClient  # Импорт клиента, где Bearer-токен
import logging
import re


# Минимальный сериализатор для отображения информации о пользователе
class MinimalUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'name', 'telegram_login']

class MasterStatisticsRequestSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(max_length=50, help_text="Telegram ID пользователя")


class MasterStatisticsResponseSerializer(serializers.Serializer):
    balance = serializers.DecimalField(max_digits=10, decimal_places=2, help_text="Баланс мастера")
    active_requests_count = serializers.IntegerField(help_text="Количество активных заявок")

logger = logging.getLogger(__name__)

class UserRegistrationSerializer(serializers.Serializer):
    """
    Сериализатор для регистрации/обновления пользователя по номеру телефона.
    Всегда создаёт новый контакт в AmoCRM, если пользователь новый.
    Принимает referral_link (содержимое команды /start ref...).
    """
    phone = serializers.CharField(required=True, help_text="Телефон (используется как ключ для update)")
    name = serializers.CharField(required=True, help_text="Имя пользователя")
    telegram_id = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    telegram_login = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    role = serializers.ChoiceField(choices=User.ROLE_CHOICES, default='Client')
    city_name = serializers.CharField(required=False, allow_blank=True, allow_null=True)

    referral_link = serializers.CharField(required=False, allow_blank=True, help_text="Содержимое команды /start (ref...)")
    service_name = serializers.CharField(required=False, allow_blank=True, help_text="Услуга мастера")
    address = serializers.CharField(required=False, allow_blank=True, help_text="Адрес мастера")
    equipment_type_name = serializers.CharField(required=False, allow_blank=True, help_text="Тип оборудования мастера")

    def parse_referral(self, referral_link: str) -> str:
        """
        Извлекает из "/start ref844860156_kl" -> '844860156',
        либо из "ref844860156_kl" -> '844860156'.
        """
        text = referral_link.strip()
        # Регулярка учитывает, что может быть "/start " в начале
        match = re.match(r"^(/start\s+)?ref(\d+)_", text)
        if match:
            return match.group(2)  # вторая группа — это \d+
        return None

    def validate(self, attrs):
        role = attrs.get('role', 'Client')
        errors = {}

        if role == 'Master':
            required_fields = ['city_name', 'service_name', 'address', 'equipment_type_name']
            for field in required_fields:
                if not attrs.get(field):
                    errors[field] = f"Необходимо указать {field.replace('_', ' ')} для мастера."
        elif role == 'Client':
            if not attrs.get('city_name'):
                errors['city_name'] = "Необходимо указать город для клиента."

        if errors:
            raise serializers.ValidationError(errors)
        return attrs

    def create(self, validated_data):
        from decimal import Decimal

        phone = validated_data['phone']
        name = validated_data['name']
        telegram_id = validated_data.get('telegram_id')
        telegram_login = validated_data.get('telegram_login')
        role = validated_data.get('role', 'Client')
        city_name = validated_data.get('city_name', '')
        referral_link = validated_data.pop('referral_link', '')
        service_name = validated_data.pop('service_name', '')
        address = validated_data.pop('address', '')
        equipment_type_name = validated_data.pop('equipment_type_name', '')

        # Парсим /start refXXX_ формулу
        telegram_ref_id = self.parse_referral(referral_link)

        with transaction.atomic():
            # 1) Ищем пользователя по сочетанию телефона и роли.
            # Это позволяет одному номеру/telegram_id иметь две записи — для клиента и мастера.
            user = User.objects.filter(phone=phone, role=role).first()
            is_new = (user is None)

            if is_new:
                logger.info(f"No user with phone={phone} and role={role}, creating new.")
                user = User.objects.create(
                    phone=phone,
                    name=name,
                    telegram_id=telegram_id,
                    telegram_login=telegram_login,
                    role=role,
                    city_name=city_name
                )
            else:
                logger.info(f"User with phone={phone} and role={role} found (id={user.id}), updating.")
                user.name = name
                user.telegram_id = telegram_id
                user.telegram_login = telegram_login
                user.role = role
                user.city_name = city_name
                user.save()

            user.referral_link = referral_link
            user.save()

            referrer_user = None
            if telegram_ref_id:
                # Пытаемся найти реферера
                try:
                    referrer_user = User.objects.get(telegram_id=telegram_ref_id)
                    # Сохраняем в модели User, если нужно
                    user.referrer = referrer_user
                    user.save()

                    # Создаём запись в ReferralLink
                    ReferralLink.objects.create(
                        referred_user=user,
                        referrer_user=referrer_user
                    )
                except User.DoesNotExist:
                    logger.warning(f"Referrer user with telegram_id={telegram_ref_id} not found.")

            # 2) Если роль=Master, создаём/обновляем профиль мастера
            if role == 'Master':
                if not hasattr(user, 'master_profile'):
                    logger.info(f"Creating Master profile for user={user.id}")
                    Master.objects.create(
                        user=user,
                        city_name=city_name,
                        service_name=service_name,
                        address=address,
                        equipment_type_name=equipment_type_name
                    )
                else:
                    master_prof = user.master_profile
                    master_prof.city_name = city_name
                    master_prof.service_name = service_name
                    master_prof.address = address
                    master_prof.equipment_type_name = equipment_type_name
                    master_prof.save()

            # 3) Реферальные бонусы (только если пользователь новый)
            if is_new and referrer_user:
                sponsor_1 = referrer_user
                sponsor_2 = sponsor_1.referrer if sponsor_1 else None

                if role == 'Master':
                    # Мастер сразу получает +500
                    if hasattr(user, 'master_profile'):
                        user.master_profile.balance = user.master_profile.balance + Decimal('500')
                        user.master_profile.save()
                    # Рефереры пока ничего не получают (по условию: при пополнении)
                else:
                    # role = 'Client'
                    # Клиенту +500 (Decimal)
                    user.balance = user.balance + Decimal('500')
                    user.save()

                    # Рефереру (1-я линия) +500
                    sponsor_1.balance = sponsor_1.balance + Decimal('500')
                    sponsor_1.save()

                    # 2-я линия +250
                    if sponsor_2:
                        sponsor_2.balance = sponsor_2.balance + Decimal('250')
                        sponsor_2.save()

            # 4) Интеграция с AmoCRM: создаём контакт
            amo_client = AmoCRMClient()
            contact_data = {
                "name": user.name,
                "custom_fields_values": []
            }

            if user.phone:
                contact_data["custom_fields_values"].append({
                    "field_code": "PHONE",
                    "values": [{"value": user.phone, "enum_code": "WORK"}]
                })

            if user.telegram_id:
                contact_data["custom_fields_values"].append({
                    "field_id": 744499,
                    "values": [{"value": user.telegram_id}]
                })

            if user.role:
                contact_data["custom_fields_values"].append({
                    "field_id": 744523,
                    "values": [{"value": user.role}]
                })

            if service_name:
                contact_data["custom_fields_values"].append({
                    "field_id": 744503,
                    "values": [{"value": service_name}]
                })

            if equipment_type_name:
                contact_data["custom_fields_values"].append({
                    "field_id": 744495,
                    "values": [{"value": equipment_type_name}]
                })

            contact_data["custom_fields_values"].append({
                "field_id": 744509,
                "values": [{"value": 0}]
            })
            contact_data["custom_fields_values"].append({
                "field_id": 744521,
                "values": [{"value": 5}]
            })

            if user.city_name:
                contact_data["custom_fields_values"].append({
                    "field_id": 744219,
                    "values": [{"value": user.city_name}]
                })
            user.save()

        return user





class ServiceRequestCreateSerializer(serializers.Serializer):
    telegram_id = serializers.CharField(required=True, help_text="Telegram ID клиента")
    service_name = serializers.CharField(required=True, help_text="Название услуги (например, 'Ремонт бытовой техники')")
    city_name = serializers.CharField(required=True, help_text="Название города")
    address = serializers.CharField(required=True, allow_blank=False, help_text="Адрес")
    description = serializers.CharField(required=False, allow_blank=True, help_text="Описание заявки")

    equipment_type = serializers.CharField(required=True, help_text="Тип оборудования (напр. стиральная машина)")
    equipment_brand = serializers.CharField(required=True, help_text="Марка оборудования (Samsung, LG и т.д.)")
    equipment_model = serializers.CharField(required=True, help_text="Модель оборудования")

    def validate_telegram_id(self, value):
        try:
            user = User.objects.get(telegram_id=value)
        except User.DoesNotExist:
            raise serializers.ValidationError("Пользователь (Client) с указанным telegram_id не найден.")
        if user.role != 'Client':
            raise serializers.ValidationError("Только пользователь с ролью 'Client' может создавать заявку.")
        return value

    def create(self, validated_data):
        telegram_id = validated_data['telegram_id']
        service_name = validated_data['service_name']  # Пример: "Ремонт бытовой техники"
        city_name = validated_data['city_name']
        address = validated_data['address']
        description = validated_data.get('description', '')

        equipment_type = validated_data['equipment_type']
        equipment_brand = validated_data['equipment_brand']
        equipment_model = validated_data['equipment_model']

        # 1. Находим клиента
        client = User.objects.get(telegram_id=telegram_id, role='Client')

        with transaction.atomic():
            # 2. Создаём заявку в локальной базе
            service_request = ServiceRequest.objects.create(
                client=client,
                service_name=service_name,
                city_name=city_name,
                address=address,
                description=description,
                status='Open',

                equipment_type=equipment_type,
                equipment_brand=equipment_brand,
                equipment_model=equipment_model
            )

            # 3. Готовим данные для лида
            #    Пример: pipeline_id=7734406, status_id=65736946 (подставьте нужные)
            pipeline_id = 7734406
            status_id = 65736946

            # Название лида: сначала service_name, затем тип, марка, модель
            lead_data = {
                "name": f"{service_name} {equipment_type} {equipment_brand} {equipment_model}",
                "status_id": status_id,
                "pipeline_id": pipeline_id,

                "_embedded": {
                    "contacts": []
                },
                "custom_fields_values": []
            }

            # Привязываем контакт, если у клиента есть amo_crm_contact_id
            if client.amo_crm_contact_id:
                lead_data["_embedded"]["contacts"].append({"id": client.amo_crm_contact_id})

            # Поля custom_fields_values (ID полей в AmoCRM берём из ваших настроек):
            lead_data["custom_fields_values"].extend([
                {
                    "field_id": 240623,  # например City
                    "values": [{"value": city_name}]
                },
                {
                    "field_id": 743447,  # например Street
                    "values": [{"value": address}]
                },
                {
                    "field_id": 743839,  # для service_name
                    "values": [{"value": service_name}]
                },
                {
                    "field_id": 725136,  # описание
                    "values": [{"value": description}]
                },
                {
                    "field_id": 240637,  # модель оборудования
                    "values": [{"value": equipment_model}]
                },
                {
                    "field_id": 240631,  # тип
                    "values": [{"value": equipment_type}]
                },
                {
                    "field_id": 240635,  # марка
                    "values": [{"value": equipment_brand}]
                }
            ])

            # 4. Создаём лид в AmoCRM
            amo_client = AmoCRMClient()
            try:
                created_lead = amo_client.create_lead(lead_data)  
                # Сохраняем ID лида
                service_request.amo_crm_lead_id = created_lead['id']
                service_request.save()
            except Exception as e:
                logger.error("Не удалось создать лид в AmoCRM: %s", e, exc_info=True)
                raise serializers.ValidationError("Ошибка при создании лида в AmoCRM, заявка откатывается.")

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


class ServiceRequestSerializer(serializers.ModelSerializer):
    client = MinimalUserSerializer(read_only=True)
    master = MasterSerializer(read_only=True)
    status = serializers.CharField(source='get_status_display', read_only=True)

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

class CheckUserByPhoneSerializer(serializers.Serializer):
    phone = serializers.CharField(required=True, help_text="Номер телефона для проверки")

    def validate_phone(self, value):
        """
        Можно добавить любую валидацию формата номера,
        но как минимум проверяем, что не пустой
        """
        if not value.strip():
            raise serializers.ValidationError("Номер телефона не должен быть пустым.")
        return value
    


class ServiceTypeSerializer(serializers.ModelSerializer):
    """
    Сериализатор для типа сервиса.
    Оставляем только 'name' + делаем массив строк для equipment_types.
    """
    equipment_types = serializers.SerializerMethodField()

    class Meta:
        model = ServiceType
        fields = ('name', 'equipment_types')

    def get_equipment_types(self, obj):
        """
        Возвращаем массив (list) с названиями связанного оборудования.
        """
        # Берём все связанные EquipmentType (через related_name='equipment_types')
        # и вытаскиваем из них поле 'name'
        return list(obj.equipment_types.values_list('name', flat=True))
    
class StatusChangeSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=True, help_text="ID лида в AmoCRM")
    status_id = serializers.IntegerField(required=True, help_text="ID статуса лида")
    pipeline_id = serializers.IntegerField(required=True, help_text="ID pipeline")
    old_status_id = serializers.IntegerField(required=True, help_text="Старый ID статуса")
    old_pipeline_id = serializers.IntegerField(required=True, help_text="Старый ID pipeline")

class LeadsSerializer(serializers.Serializer):
    status = serializers.ListField(child=StatusChangeSerializer())

class AmoCRMWebhookSerializer(serializers.Serializer):
    leads = LeadsSerializer()
    account = serializers.DictField()

    def validate(self, attrs):
        leads = attrs.get('leads', {}).get('status', [])
        if not leads:
            raise serializers.ValidationError("No leads found in webhook data.")
        
        for lead in leads:
            if 'id' not in lead or 'status_id' not in lead:
                raise serializers.ValidationError("Each lead must have 'id' and 'status_id'.")
        
        return attrs