from decimal import Decimal

from serviceapp.models import User
from serviceapp.utils import create_bonus_tx


BONUS_L1 = Decimal("500")
BONUS_L2 = Decimal("250")


def _recipient(u: User):
    """client → сам user, master  → его master_profile."""
    return u.master_profile if u.role == "Master" else u


def apply_first_deposit_bonus(invited: User) -> None:
    """Начисляем бонус его реферам (1-я и 2-я линии)."""
    s1 = invited.referrer
    if not s1:                 # пришёл без рефера
        return

    s2 = s1.referrer

    create_bonus_tx(_recipient(s1), BONUS_L1,
                    "Бонус 1-й линии за первое пополнение приглашённого")
    if s2:
        create_bonus_tx(_recipient(s2), BONUS_L2,
                        "Бонус 2-й линии за первое пополнение приглашённого")