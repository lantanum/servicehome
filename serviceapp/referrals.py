from decimal import Decimal

from serviceapp.models import User
from serviceapp.utils import create_bonus_tx


BONUS_L1 = Decimal("500")   # то, что получает реферер 1-й линии
BONUS_L2 = Decimal("250")   # «дедушке», если есть

def _recipient(u: User):
    """Баланс-получатель: Master → master_profile, Client → сам user."""
    return u.master_profile if u.role == "Master" else u


def apply_first_deposit_bonus(invited_user: User) -> None:
    """
    ✓ Вызывается ТОЛЬКО при подтверждении *первого* депозита invited_user.
    ✓ Начисляет бонус(ы) тем, кто пригласил этого пользователя – по его роли:

        invited_user.role == 'Master'
            – бонусы кладём master_profile.referrer(-ам)
        invited_user.role == 'Client'
            – бонусы кладём referrer.balance
    """
    sponsor_1 = invited_user.referrer
    if not sponsor_1:           # никто не приглашал – ничего не делаем
        return

    sponsor_2 = sponsor_1.referrer

    # -- первая линия -------------------------------------------------
    recipient_1 = (
        sponsor_1.master_profile          # мастер приглашал мастера → на счёт профиля
        if sponsor_1.role == "Master"
        else sponsor_1                    # клиент приглашал клиента → на счёт User
    )
    create_bonus_tx(recipient_1, BONUS_L1, "Бонус 1-й линии за первое пополнение реферала")

    # -- вторая линия --------------------------------------------------
    if sponsor_2:
        recipient_2 = (
            sponsor_2.master_profile
            if sponsor_2.role == "Master"
            else sponsor_2
        )
        create_bonus_tx(recipient_2, BONUS_L2, "Бонус 2-й линии за первое пополнение реферала")