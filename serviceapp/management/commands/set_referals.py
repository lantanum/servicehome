# serviceapp/management/commands/set_referrals.py
from __future__ import annotations
import logging
import re
from typing import Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction

from serviceapp.models import ReferralLink, User

logger = logging.getLogger(__name__)
DIGIT_RE = re.compile(r"^\s*(\d+)\s*$")      # «только цифры» + пробелы

ROLE_FOR_REF = {             # у кого какого типа должен быть пригласитель
    "Master": "Master",
    "Client": "Client",
    "Admin":  "Client",
}

class Command(BaseCommand):
    help = (
        "Проставляет referrer пользователям по полю referral_link.\n"
        "Строка referral_link содержит ТОЛЬКО цифрами telegram-ID пригласителя.\n"
        "Если по этому ID найдено несколько подходящих пользователей – выводит их в консоль."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--dry",
            action="store_true",
            help="Сухой прогон – только отчёт, без изменения БД.",
        )

    # ------------------------------------------------------------------ #
    def handle(self, *args, **opts):
        dry = opts["dry"]

        total = updated = skipped = not_found = self_ref = duplicates = 0
        qs = User.objects.exclude(referral_link__isnull=True).exclude(referral_link__exact="")

        for user in qs:
            total += 1
            tg_id = self._extract_digits(user.referral_link)
            if not tg_id:
                skipped += 1
                continue

            target_role = ROLE_FOR_REF.get(user.role, "Client")
            ref_qs = User.objects.filter(telegram_id=tg_id, role=target_role)

            if not ref_qs.exists():
                not_found += 1
                continue
            if ref_qs.count() > 1:
                duplicates += 1
                self._print_dupes(tg_id, ref_qs)
                continue

            referrer = ref_qs.first()
            if referrer.id == user.id:
                self_ref += 1
                continue
            if user.referrer_id == referrer.id:
                continue  # уже назначен

            if dry:
                updated += 1
                self.stdout.write(f"[DRY] user {user.id} ({user.role}) → ref {referrer.id}")
                continue

            with transaction.atomic():
                user.referrer = referrer
                user.save(update_fields=["referrer"])
                ReferralLink.objects.get_or_create(
                    referred_user=user,
                    referrer_user=referrer,
                )
                updated += 1
                logger.info("User %s < referrer %s", user.id, referrer.id)

        mode = "DRY-RUN finished" if dry else "Finished"
        self.stdout.write(
            self.style.SUCCESS(
                f"{mode}: total={total}, updated={updated}, skipped={skipped}, "
                f"not_found={not_found}, duplicates={duplicates}, self_ref={self_ref}"
            )
        )

    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_digits(raw: str) -> Optional[str]:
        """Если строка состоит только из цифр (с пробелами) – вернуть их, иначе None."""
        m = DIGIT_RE.match(raw or "")
        return m.group(1) if m else None

    def _print_dupes(self, tg_id: str, dupes_qs):
        """Выводит сведения о дубликатах в консоль."""
        self.stdout.write(
            self.style.WARNING(
                f"[DUPLICATE] telegram_id={tg_id} найден у {dupes_qs.count()} пользователей:"
            )
        )
        for u in dupes_qs.order_by("id"):
            self.stdout.write(f"    id={u.id}, role={u.role}, name='{u.name}'")
