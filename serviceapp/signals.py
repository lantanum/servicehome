# file: myapp/signals.py
import requests
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from rest_framework.renderers import JSONRenderer

from .models import ServiceType, EquipmentType
from .serializers import ServiceTypeSerializer
import logging

logger = logging.getLogger(__name__)


def send_service_equipment_data():
    service_types = ServiceType.objects.all()
    serializer = ServiceTypeSerializer(service_types, many=True)
    data = {
        "service_types": serializer.data
    }
    
    url = "https://sambot.ru/reactions/2986626/start"
    
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
