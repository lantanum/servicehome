from __future__ import annotations

"""
Импорт лидов из AmoCRM в таблицу ServiceRequest.

▪ Сохраняем/обновляем только саму заявку и клиента – никаких комиссий,
  рассылок и прочих бизнес-процессов.
▪ Если у лида нет связанного контакта, такой лид пропускается.
▪ Опциональный аргумент --from-date (YYYY-MM-DD) ограничивает выборку
  по дате создания лида в AmoCRM.
"""

import logging
from datetime import datetime, timezone, date
from typing import Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction

from serviceapp.amocrm_client import AmoCRMClient
from serviceapp.business import save_lead

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Импортирует лиды AmoCRM в ServiceRequest "
        "(только сохранение заявки/клиента, без доп-логики). "
        "Лиды без контакта клиента пропускаются."
    )

    PAGE_SIZE = 250  # лимит API v4

    # ------------------------------------------------------------------
    # CLI-аргументы
    # ------------------------------------------------------------------
    def add_arguments(self, parser: CommandParser):
        parser.add_argument(
            "--from-date",
            dest="from_date",
            type=str,
            help="Импортировать лиды, созданные начиная с этой даты (YYYY-MM-DD).",
        )

    # ------------------------------------------------------------------
    # Главная точка входа
    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from_date: str | None = options.get("from_date")
        created_from_ts: int | None = None

        # преобразуем дату из аргумента в UNIX-время (UTC)
        if from_date:
            try:
                dt: date = datetime.strptime(from_date, "%Y-%m-%d").date()
                created_from_ts = int(
                    datetime.combine(dt, datetime.min.time())
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                )
            except ValueError:
                self.stderr.write(
                    self.style.ERROR("--from-date должен быть в формате YYYY-MM-DD")
                )
                return

        client = AmoCRMClient()
        page = 1
        new_cnt = upd_cnt = skipped_cnt = err_cnt = 0

        extra: dict | None = None
        if created_from_ts is not None:
            extra = {"filter[created_at][from]": created_from_ts}

        # ------------------------------------------------------------------
        # Пагинация по лидам AmoCRM
        # ------------------------------------------------------------------
        while True:
            leads = client.list_leads(
                page=page,
                limit=self.PAGE_SIZE,
                with_="contacts",
                extra=extra,
            )
            if not leads:
                break

            for lead in leads:
                try:
                    with transaction.atomic():
                        result = self._sync_single_lead(lead, client)

                        if result is None:          # лид пропущен
                            skipped_cnt += 1
                        elif result:                # created == True
                            new_cnt += 1
                        else:                       # created == False
                            upd_cnt += 1
                except Exception as exc:
                    err_cnt += 1
                    logger.exception(
                        "Lead %s processing failed: %s", lead.get("id"), exc
                    )

            page += 1

        # итоговая строка
        self.stdout.write(
            self.style.SUCCESS(
                f"Finished. new={new_cnt}, updated={upd_cnt}, "
                f"skipped={skipped_cnt}, errors={err_cnt}"
            )
        )

    # ------------------------------------------------------------------
    # Сохранение одного лида
    # ------------------------------------------------------------------
    def _sync_single_lead(
        self,
        lead: dict,
        amoclient: AmoCRMClient,
    ) -> Optional[bool]:
        """
        Возвращает:
            • True  – заявка создана
            • False – заявка обновлена
            • None  – лид пропущен (нет контакта)
        """
        sr, created = save_lead(lead, amoclient)

        # save_lead отдаёт sr == None, если у лида нет связанного контакта
        if sr is None:
            logger.info("Лид %s пропущен: нет связанного контакта", lead["id"])
            return None

        return created
