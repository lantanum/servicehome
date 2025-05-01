# serviceapp/management/commands/delete_user_by_telegram.py
from __future__ import annotations

import logging
from typing import Iterable

from django.core.management.base import BaseCommand, CommandParser, CommandError
from django.db import transaction, IntegrityError
from django.db.models import ProtectedError

from serviceapp.models import (
    User,
    ServiceRequest,
    ReferralLink,
    Transaction,
    Master,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Удаляет *ВСЕ* аккаунты User с указанным telegram-ID.

    ▸ по умолчанию просит подтверждение;
    ▸ --yes  — выполняет без вопросов;
    ▸ --dry  — только отчёт, без изменений.
    """

    help = "Удаляет пользователя(ей) по telegram_id вместе с зависимостями."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("telegram_id", type=str, help="Telegram-ID пользователя")
        parser.add_argument(
            "--yes",
            action="store_true",
            help="Не задавать вопросов, удалить сразу.",
        )
        parser.add_argument(
            "--dry",
            action="store_true",
            help="Сухой запуск — только отчёт, без удаления.",
        )

    # ------------------------------------------------------------------ #
    def handle(self, *args, **opts):
        tg_id: str = opts["telegram_id"]
        sure: bool = opts["yes"]
        dry: bool = opts["dry"]

        users = list(User.objects.filter(telegram_id=tg_id))
        if not users:
            raise CommandError(f"Пользователи с telegram_id={tg_id} не найдены.")

        self.stdout.write(
            self.style.WARNING(
                f"Найдено {len(users)} пользователь(ей) с telegram_id={tg_id}: "
                f"{', '.join(str(u.id) for u in users)}"
            )
        )

        if not dry and not sure:
            answer = input("Точно удалить? (yes/NO): ").strip().lower()
            if answer != "yes":
                self.stdout.write(self.style.ERROR("Отменено."))
                return

        deleted_total = 0
        for user in users:
            if dry:
                self._print_user_info(user)
                continue

            try:
                with transaction.atomic():
                    self._remove_related_objects(user)
                    deleted, _ = user.delete()      # каскадное удаление
                    deleted_total += deleted
                    logger.info("User %s (tg=%s) удалён, удалено объектов: %s", user.id, tg_id, deleted)
            except ProtectedError as exc:
                self.stderr.write(
                    self.style.ERROR(
                        f"Нельзя удалить User {user.id}: защищён связью {exc.protected_objects}."
                    )
                )
            except IntegrityError as exc:
                self.stderr.write(self.style.ERROR(f"Ошибка IntegrityError: {exc}"))

        if dry:
            self.stdout.write(self.style.SUCCESS("DRY-RUN завершён. Ничего не удалено."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Готово. Удалено объектов: {deleted_total}."))

    # ------------------------------------------------------------------ #
    def _print_user_info(self, user: User) -> None:
        """Краткая сводка о том, что будет удалено."""
        related_requests = ServiceRequest.objects.filter(client=user) | ServiceRequest.objects.filter(master__user=user)
        self.stdout.write(
            f"[DRY] User id={user.id} ({user.role}), "
            f"ServiceRequests={related_requests.count()}, "
            f"ReferralLinks={user.referrer_links.count()} / {ReferralLink.objects.filter(referred_user=user).count()}, "
            f"Transactions={user.transactions.count()}"
        )


    def _remove_related_objects(self, user: User) -> None:
        """
        Полностью удаляет все объекты, завязанные на user
        (ServiceRequest, Master-profile, ReferralLink, Transaction и т.д.).

        Делается в правильном порядке, чтобы не словить ProtectedError.
        """
        # ➊ Сначала удаляем заявки, где user – клиент или мастер
        ServiceRequest.objects.filter(client=user).delete()

        if user.role == "Master":
            try:
                master_profile: Master = user.master_profile
            except Master.DoesNotExist:
                master_profile = None

            if master_profile:
                # Заявки, где этот мастер был исполнителем
                ServiceRequest.objects.filter(master=master_profile).delete()

                # Транзакции мастера (после удаления заявок FK service_request уже NULL)
                Transaction.objects.filter(master=master_profile).delete()

                # Сам профиль мастера
                master_profile.delete()

        # ➋ Referral-связи
        ReferralLink.objects.filter(referrer_user=user).delete()
        ReferralLink.objects.filter(referred_user=user).delete()

        # ➌ Транзакции клиента (если роль = Client)
        Transaction.objects.filter(client=user).delete()