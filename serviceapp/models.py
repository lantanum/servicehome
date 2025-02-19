from decimal import Decimal
from django.db import models
class User(models.Model):
    ROLE_CHOICES = [
        ('Client', 'Client'),
        ('Master', 'Master'),
        ('Admin', 'Admin'),
    ]

    name = models.CharField(max_length=255, help_text="Имя пользователя")
    phone = models.CharField(max_length=255, null=True, blank=True, help_text="Телефон")
    telegram_id = models.CharField(max_length=255, null=True, blank=True, unique=True, help_text="Telegram ID пользователя")
    telegram_login = models.CharField(max_length=255, null=True, blank=True, unique=True, help_text="Telegram логин пользователя")
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default='Client', help_text="Роль пользователя (Client/Master/Admin)")
    city_name = models.CharField(max_length=255, null=True, blank=True, help_text="Город пользователя")
    created_at = models.DateTimeField(auto_now_add=True)

    # Поле для хранения ID контакта в AmoCRM, если нужно
    amo_crm_contact_id = models.IntegerField(null=True, blank=True, unique=True, help_text="ID контакта в AmoCRM")

    # Сырая реферальная строка, пришедшая из '/start ref...'
    referral_link = models.CharField(null=True, blank=True, max_length=255, help_text="Содержимое команды /start (реф. строка)")

    # Ссылка на реферера, если нужно хранить, кто пригласил
    referrer = models.ForeignKey('self', null=True, blank=True, on_delete=models.SET_NULL, help_text="Кто пригласил пользователя")
    balance = models.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        default=Decimal('0.00'),  # <-- Так лучше
        help_text="Баланс клиента"
    )


    def __str__(self):
        return f"{self.name} ({self.role})"


class Master(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='master_profile')
    address = models.TextField(null=True, blank=True)
    level = models.IntegerField(default=1, help_text="Уровень мастера")
    rating = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    balance = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    city_name = models.CharField(max_length=255, null=True, blank=True, help_text="Название города")
    service_name = models.CharField(max_length=255, null=True, blank=True, help_text="Название услуги")
    equipment_type_name = models.CharField(max_length=255, null=True, blank=True, help_text="Тип оборудования мастера")  # Новое строковое поле
    balance = models.DecimalField(max_digits=10, decimal_places=2, default=0.0, help_text="Баланс мастера")
    def __str__(self):
        return f"Master: {self.user.name}"


class ServiceRequest(models.Model):
    STATUS_CHOICES = [
        ('Open', 'Новая заявка'),
        ('In Progress', 'Взяли в работу'),
        ('Completed', 'Успешно реализовано'),
        ('Cancelled', 'Отказ от ремонта'),
        ('Free', 'Свободная заявка'),
        ('AwaitingClosure', 'Ожидает закрытия'),  # <-- новый статус
        ('Closed', 'Закрыто'),                   # <-- ещё один новый статус
        ('QualityControl', 'Контроль качества'), 
    ]


    client = models.ForeignKey(User, on_delete=models.CASCADE, related_name='client_requests')
    master = models.ForeignKey(Master, on_delete=models.SET_NULL, null=True, blank=True, related_name='master_requests')

    # Новые поля для оборудования
    equipment_type = models.CharField(max_length=255, null=True, blank=True, help_text="Тип оборудования (например, стиральная машина)")
    equipment_brand = models.CharField(max_length=255, null=True, blank=True, help_text="Марка оборудования (например, LG, Samsung)")
    equipment_model = models.CharField(max_length=255, null=True, blank=True, help_text="Модель оборудования")

    # Поля, что были раньше
    service_name = models.CharField(max_length=255, null=True, blank=True)
    city_name = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Open')
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    client_rating = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    address = models.TextField(null=True, blank=True)
    cancellation_reason = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)

    amo_crm_lead_id = models.IntegerField(null=True, blank=True, unique=True, help_text="ID лида в AmoCRM")
    amo_status_code = models.IntegerField(null=True, blank=True, help_text="Внешний статус заявки (например, из AmoCRM)")

    # Новые поля:
    warranty = models.CharField(max_length=255, null=True, blank=True, help_text="Гарантия (например, 6 месяцев)")
    spare_parts_spent = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True,
                                           help_text="Сумма, потраченная на запчасти")
    comment_after_finish = models.TextField(null=True, blank=True, help_text="Комментарий мастера после завершения работ")

    start_date = models.DateTimeField(null=True, blank=True, help_text="Дата начала работ")
    end_date = models.DateTimeField(null=True, blank=True, help_text="Дата окончания работ")
    crm_operator_comment = models.TextField(null=True, blank=True, help_text="Комментарий оператора из AmoCRM")
    deal_success = models.CharField(max_length=255, null=True, blank=True, help_text = "Успех сделки")

    def __str__(self):
        return f"Request {self.id} by {self.client.name}"



class Transaction(models.Model):
    TRANSACTION_CHOICES = [
        ('Deposit', 'Deposit'),
        ('Withdrawal', 'Withdrawal'),
        ('Penalty', 'Penalty'),
    ]

    TRANSACTION_STATUS = [
        ('Pending', 'Pending'),
        ('Confirmed', 'Confirmed'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    transaction_type = models.CharField(max_length=20, choices=TRANSACTION_CHOICES)
    status = models.CharField(
        max_length=20, 
        choices=TRANSACTION_STATUS, 
        default='Pending'
    )
    reason = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.transaction_type} {self.amount} ({self.status}) for {self.user}"



class RatingLog(models.Model):
    master = models.ForeignKey(Master, on_delete=models.CASCADE)
    service_request = models.ForeignKey(ServiceRequest, on_delete=models.CASCADE)
    rating_change = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    change_reason = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)


class ReferralLink(models.Model):
    referred_user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='referred_links')
    referrer_user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='referrer_links')
    joined_at = models.DateTimeField(auto_now_add=True)



class InteractionLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    action_description = models.CharField(max_length=255, null=True, blank=True)
    action_timestamp = models.DateTimeField(auto_now_add=True)




class ServiceType(models.Model):
    name = models.CharField(
        max_length=255,
        unique=True,
        help_text="Название типа сервиса"
    )
    # Три новых поля для комиссий
    commission_level_1 = models.DecimalField(
        max_digits=7,
        decimal_places=2,
        null=True, 
        blank=True,
        verbose_name="Комиссия 1-го уровня",
        help_text="Комиссия 1-го уровня (в условных единицах, например, в рублях или процентах)",
    )
    commission_level_2 = models.DecimalField(
        max_digits=7,
        decimal_places=2,
        null=True, 
        blank=True,
        verbose_name="Комиссия 2-го уровня",
        help_text="Комиссия 2-го уровня (в условных единицах, например, в рублях или процентах)",
    )
    commission_level_3 = models.DecimalField(
        max_digits=7,
        decimal_places=2,
        null=True, 
        blank=True,
        verbose_name="Комиссия 3-го уровня",
        help_text="Комиссия 3-го уровня (в условных единицах, например, в рублях или процентах)",
    )

    def __str__(self):
        return self.name


class EquipmentType(models.Model):
    name = models.CharField(
        max_length=255,
        unique=True,
        help_text="Название типа оборудования"
    )
    service_type = models.ForeignKey(
        ServiceType,
        on_delete=models.CASCADE,
        related_name='equipment_types',
        default=1  # <-- нужный ID или метод
    )
    

    def __str__(self):
        return self.name


class Settings(models.Model):
    commission_level1 = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=0.0,
        help_text="Процент комиссии для уровня 1"
    )
    commission_level2 = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=0.0,
        help_text="Процент комиссии для уровня 2"
    )
    commission_level3 = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=0.0,
        help_text="Процент комиссии для уровня 3"
    )

    # Новые поля
    max_requests_level1 = models.PositiveIntegerField(
        default=1,
        help_text="Максимальное число заявок в работе для уровня 1"
    )
    max_requests_level2 = models.PositiveIntegerField(
        default=3,
        help_text="Максимальное число заявок в работе для уровня 2"
    )
    max_requests_level3 = models.PositiveIntegerField(
        default=5,
        help_text="Максимальное число заявок в работе для уровня 3"
    )

    amocrm_bearer_token = models.TextField(
        help_text="Токен для аутентификации в AmoCRM",
        default=''
    )

    def __str__(self):
        return (
            f"Настройки системы:\n"
            f"Комиссия L1: {self.commission_level1}%, L2: {self.commission_level2}%, L3: {self.commission_level3}%\n"
            f"Макс. заявки L1: {self.max_requests_level1}, "
            f"L2: {self.max_requests_level2}, "
            f"L3: {self.max_requests_level3}"
        )
