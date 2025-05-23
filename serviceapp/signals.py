# file: myapp/signals.py
from decimal import Decimal
import requests
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from rest_framework.renderers import JSONRenderer

from serviceapp.utils import create_bonus_tx
from serviceapp.views import recalc_master_rating

from .models import ServiceRequest, ServiceType, EquipmentType, Settings, Transaction, User
from .serializers import ServiceTypeSerializer
import logging

logger = logging.getLogger(__name__)


def send_service_equipment_data():
    service_types = ServiceType.objects.all()
    serializer = ServiceTypeSerializer(service_types, many=True)
    data = {
        "service_types": serializer.data
    }
    
    url = "https://sambot.ru/reactions/3138748/start?token=yhvtlmhlqbj"
    
    # Логируем, какие данные пытаемся отправить
    logger.info("Отправка данных на %s: %s", url, data)
    
    try:
        response = requests.post(url, json=data, timeout=5)
        response.raise_for_status()
        # Логируем успешный результат
        logger.info("Успешно отправлены данные на %s. Код ответа: %s", url, response.status_code)
    except requests.RequestException as e:
        # Логируем ошибку
        logger.error("Ошибка при отправке данных на %s: %s", url, str(e))



def award_level_bonus(user: User, new_level: int) -> None:
    """
    Записывает бонус уровня как Confirmed-транзакцию.
    """
    if user.role != "Client":
        return

    settings = Settings.objects.first()
    if not settings:
        return

    base_bonus = {
        1: settings.bonus_level1,
        2: settings.bonus_level2,
        3: settings.bonus_level3,
        4: settings.bonus_level4,
    }.get(new_level, Decimal("0"))

    invites_cnt = user.referrer_links.count()
    total_bonus = base_bonus + (settings.bonus_per_invite * invites_cnt)

    create_bonus_tx(
        user,
        total_bonus,
        f"Бонус за достижение уровня {new_level} (+{invites_cnt} приглаш.)"
    )

    user.client_level = new_level
    user.save(update_fields=["client_level"])


@receiver(post_save, sender=ServiceType)
@receiver(post_delete, sender=ServiceType)
def service_type_changed(sender, instance, **kwargs):
    """
    Срабатывает при любом изменении (создание, обновление, удаление) ServiceType.
    """
    send_service_equipment_data()


@receiver(post_save, sender=EquipmentType)
@receiver(post_delete, sender=EquipmentType)
def equipment_type_changed(sender, instance, **kwargs):
    """
    Срабатывает при любом изменении (создание, обновление, удаление) EquipmentType.
    """
    send_service_equipment_data()



def recalc_client_balance(client):
    sign = {
        'Deposit': Decimal(1),
        'Comission': Decimal(-1),
        'Penalty': Decimal(-1)
    }
    total = Decimal('0.00')
    for tx in client.transactions.filter(status='Confirmed'):
        total += sign.get(tx.transaction_type, Decimal('0.00')) * tx.amount
    client.balance = total
    client.save(update_fields=['balance'])

def recalc_master_balance(master):
    sign = {
        'Deposit': Decimal(1),
        'Comission': Decimal(-1),
        'Penalty': Decimal(-1),
        'Bonus': Decimal(1),
    }
    total = Decimal('0.00')
    for tx in master.transactions.filter(status='Confirmed'):
        total += sign.get(tx.transaction_type, Decimal('0.00')) * tx.amount
    master.balance = total
    master.save(update_fields=['balance'])


@receiver(post_save, sender=Transaction)
def update_balance_on_transaction_save(sender, instance, **kwargs):
    if instance.client:
        recalc_client_balance(instance.client)
    elif instance.master:
        recalc_master_balance(instance.master)

@receiver(post_delete, sender=Transaction)
def update_balance_on_transaction_delete(sender, instance, **kwargs):
    if instance.client:
        recalc_client_balance(instance.client)
    elif instance.master:
        recalc_master_balance(instance.master)

@receiver(post_save, sender=ServiceRequest)
@receiver(post_delete, sender=ServiceRequest)
def update_master_rating_on_service_request_change(sender, instance, **kwargs):
    """
    Срабатывает при любом сохранении или удалении ServiceRequest.
    Пересчитывает рейтинг мастера, если у заявки есть мастер.
    """
    if instance.master:
        recalc_master_rating(instance.master)