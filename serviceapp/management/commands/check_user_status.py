from django.core.management.base import BaseCommand
from serviceapp.models import User

class Command(BaseCommand):
    help = 'Показывает статус пользователя по telegram_id'

    def add_arguments(self, parser):
        parser.add_argument('telegram_id', type=str)

    def handle(self, *args, **kwargs):
        telegram_id = kwargs['telegram_id']

        try:
            user = User.objects.get(telegram_id=telegram_id)
            self.stdout.write(self.style.SUCCESS(f'User {user.name} is active: {user.is_active}'))
        except User.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'User with telegram_id {telegram_id} not found'))
