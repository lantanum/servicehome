# serviceapp/deposits.py
from decimal import Decimal
from serviceapp.models import User
from serviceapp.utils import create_bonus_tx


def _recipient(u):
    return u.master_profile if u.role == "Master" else u


def apply_first_deposit_bonus(master_user: User) -> None:
    """
    Вызывается ТОЛЬКО при первом Confirmed-депозите мастера.
    Спонсору (1-я линия) — 500  
    Спонсору (2-я линия) — 250
    """
    sponsor_1 = master_user.referrer
    if not sponsor_1:
        return

    sponsor_2 = sponsor_1.referrer

    create_bonus_tx(
        _recipient(sponsor_1),
        Decimal("500"),
        "Бонус 1-й линии за первое пополнение приглашённого мастера"
    )

    if sponsor_2:
        create_bonus_tx(
            _recipient(sponsor_2),
            Decimal("250"),
            "Бонус 2-й линии за первое пополнение приглашённого мастера"
        )
