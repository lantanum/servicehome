from django.db import models

class User(models.Model):
    ROLE_CHOICES = [
        ('Client', 'Client'),
        ('Master', 'Master'),
        ('Admin', 'Admin'),
    ]

    name = models.CharField(max_length=255)
    phone = models.CharField(max_length=255, null=True, blank=True)
    telegram_id = models.CharField(max_length=255, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default='Client')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name} ({self.role})"


class City(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name


class Master(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    city = models.ForeignKey(City, on_delete=models.CASCADE)
    address = models.TextField(null=True, blank=True)
    rating = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    balance = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)

    def __str__(self):
        return f"Master: {self.user.name}"


class Service(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name


class MasterService(models.Model):
    master = models.ForeignKey(Master, on_delete=models.CASCADE)
    service = models.ForeignKey(Service, on_delete=models.CASCADE)


class ServiceRequest(models.Model):
    STATUS_CHOICES = [
        ('Open', 'Open'),
        ('In Progress', 'In Progress'),
        ('Completed', 'Completed'),
        ('Cancelled', 'Cancelled'),
    ]

    client = models.ForeignKey(User, on_delete=models.CASCADE, related_name='client_requests')
    master = models.ForeignKey(Master, on_delete=models.SET_NULL, null=True, blank=True, related_name='master_requests')
    service = models.ForeignKey(Service, on_delete=models.CASCADE)
    city = models.ForeignKey(City, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Open')
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    client_rating = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    address = models.TextField(null=True, blank=True)
    cancellation_reason = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)


class Transaction(models.Model):
    TRANSACTION_CHOICES = [
        ('Deposit', 'Deposit'),
        ('Withdrawal', 'Withdrawal'),
        ('Penalty', 'Penalty'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    transaction_type = models.CharField(max_length=20, choices=TRANSACTION_CHOICES)
    reason = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)


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
    bonus_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)


class InteractionLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    action_description = models.CharField(max_length=255, null=True, blank=True)
    action_timestamp = models.DateTimeField(auto_now_add=True)
