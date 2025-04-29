import requests
import logging
from django.core.management.base import BaseCommand
from serviceapp.models import Master

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Отправляет уведомление о мастерах с отрицательным балансом"

    def handle(self, *args, **options):
        masters = Master.objects.filter(balance__lt=0)
        telegram_ids = [m.user.telegram_id for m in masters if m.user and m.user.telegram_id]
        payload = {"masters": telegram_ids}
        
        try:
            response = requests.post(
                'https://sambot.ru/reactions/3138790/start?token=yhvtlmhlqbj',
                json=payload,
                timeout=10
            )
            if response.status_code == 200:
                self.stdout.write(self.style.SUCCESS(f"Уведомление отправлено успешно для мастеров: {telegram_ids}"))
                logger.info(f"Уведомление отправлено успешно для мастеров: {telegram_ids}")
            else:
                self.stderr.write(f"Ошибка отправки уведомления: {response.status_code} - {response.text}")
                logger.error(f"Ошибка отправки уведомления: {response.status_code} - {response.text}")
        except Exception as ex:
            self.stderr.write(f"Error sending negative balance notification: {ex}")
            logger.exception(f"Error sending negative balance notification: {ex}")
