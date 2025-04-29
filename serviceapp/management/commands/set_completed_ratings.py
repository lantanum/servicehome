from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
import logging

from serviceapp.models import ServiceRequest

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Проставляет всем заявкам в статусе «Completed» оценки 5-5-5 "
        "(quality_rating, competence_rating, recommendation_rating)."
    )

    # ────────────────────────────────────────────────────────────────
    # CLI-аргументы (опционально: --dry-run)
    # ────────────────────────────────────────────────────────────────
    def add_arguments(self, parser: CommandParser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Ничего не изменять в БД, а только вывести список заявок, "
                 "которым были бы выставлены рейтинги.",
        )

    # ────────────────────────────────────────────────────────────────
    # Основная логика
    # ────────────────────────────────────────────────────────────────
    def handle(self, *args, **options):
        dry_run: bool = options["dry_run"]

        qs = ServiceRequest.objects.filter(status="Completed")
        total = qs.count()

        if total == 0:
            self.stdout.write(self.style.WARNING("Заявок со статусом Completed не найдено."))
            return

        # Если нужен лишь «предпросмотр»
        if dry_run:
            ids = list(qs.values_list("id", flat=True))
            self.stdout.write(
                self.style.SUCCESS(
                    f"[DRY-RUN]  Нашёл {total} заявок. "
                    f"Всем им будут проставлены рейтинги 5-5-5:\n{ids}"
                )
            )
            return

        # Обновляем одним запросом — быстро и без лишних save()
        with transaction.atomic():
            updated = qs.update(
                quality_rating=5,
                competence_rating=5,
                recommendation_rating=5,
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Готово! Обновлено {updated} из {total} заявок (всем выставлены оценки 5-5-5)."
            )
        )
        logger.info("Команда set_completed_ratings: обновлено %s заявок.", updated)
