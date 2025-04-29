# serviceapp/management/commands/set_positive_outcome_for_all.py
from django.core.management.base import BaseCommand
from django.db import transaction
from serviceapp.models import WorkOutcome, ServiceRequest


class Command(BaseCommand):
    help = (
        "Устанавливает всем существующим заявкам "
        "WorkOutcome «Клиент довольный/Оставит отзыв»"
    )

    def handle(self, *args, **options):
        # 1. Находим нужный WorkOutcome (без учёта регистра)
        outcome = (
            WorkOutcome.objects
            .filter(outcome_name__iexact="Клиент довольный/Оставит отзыв")
            .first()
        )

        if not outcome:
            self.stderr.write(
                self.style.ERROR(
                    "WorkOutcome «Клиент довольный/Оставит отзыв» не найден. "
                    "Создайте запись в админке и повторите."
                )
            )
            return

        # 2. Массово обновляем заявки (без лишних сигналов/циклов)
        with transaction.atomic():
            updated = ServiceRequest.objects.update(work_outcome=outcome)

        self.stdout.write(
            self.style.SUCCESS(
                f"Готово! Для {updated} заявок установлен итог "
                f"«{outcome.outcome_name}» (id={outcome.id})."
            )
        )
