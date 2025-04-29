from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.utils.timezone import now

from serviceapp.models import User, Master, Transaction

import logging
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Импорт начального баланса мастеров из .txt / .csv.

    Формат строки (любой из трёх):
        844860156 11534
        844860156,11534
        844860156 , 11534

    Если сумма > 0 – создаём Deposit/Confirmed.
    Если сумма < 0 – создаём Penalty/Confirmed (можно поменять ниже).
    Пустые или «0» пропускаются.
    """

    help = "Создаёт подтверждённые транзакции начального баланса мастеров"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "txt_path",
            type=str,
            help="Путь к balance-файлу (txt / csv)",
        )

    # ────────────────────────────────────────────────────────────
    def handle(self, *args, **opts):
        path = Path(opts["txt_path"]).expanduser()
        if not path.exists():
            self.stderr.write(self.style.ERROR(f"Файл {path} не найден"))
            return

        created = skipped = bad = 0

        with path.open(encoding="utf-8") as fh, transaction.atomic():
            for i, raw in enumerate(fh, start=1):
                line = raw.strip()
                if not line:
                    continue

                # --- универсальный split (запятая или пробел) ---
                if "," in line:
                    parts = [p.strip() for p in line.split(",", 1)]
                else:
                    parts = line.split(None, 1)

                if len(parts) != 2:
                    self._warn(i, f"некорректная строка «{raw.rstrip()}»")
                    bad += 1
                    continue

                tg_id, amt_str = parts
                amt_str = amt_str.replace(" ", "")
                if not amt_str:
                    skipped += 1
                    self._warn(i, "пустая сумма – пропуск")
                    continue

                try:
                    amount = Decimal(amt_str)
                except InvalidOperation:
                    self._warn(i, f"не число: «{amt_str}» – пропуск")
                    bad += 1
                    continue

                if amount == 0:
                    skipped += 1
                    continue

                # --- ищем мастера ---
                user = User.objects.filter(telegram_id=tg_id, role="Master").first()
                master = getattr(user, "master_profile", None)
                if master is None:
                    self._warn(i, f"мастер с tg_id={tg_id} не найден – пропуск")
                    skipped += 1
                    continue

                tr_type = "Deposit" if amount > 0 else "Penalty"

                # дубль?
                if Transaction.objects.filter(
                    master=master,
                    transaction_type=tr_type,
                    amount=abs(amount),
                    status="Confirmed",
                ).exists():
                    skipped += 1
                    continue

                Transaction.objects.create(
                    master=master,
                    amount=abs(amount),
                    transaction_type=tr_type,
                    status="Confirmed",
                    reason="Импорт начального баланса",
                    created_at=now(),
                )
                created += 1
                logger.info(
                    "[balance-import] %s %s для master=%s (user.tg=%s)",
                    tr_type,
                    amount,
                    master.id,
                    tg_id,
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"Готово: создано {created}, пропущено {skipped}, ошибок {bad}"
            )
        )

    # ────────────────────────────────────────────────────────────
    def _warn(self, line, msg):
        logger.warning("Line %s: %s", line, msg)
