# ────────────────────────────────────────────────────────────────
#  Константы (если удобно, вынесите в settings / БД)
# ────────────────────────────────────────────────────────────────
from decimal import Decimal

from serviceapp.models import User
from serviceapp.utils import create_bonus_tx
BONUS_SELF  = Decimal("500")   # новичку
BONUS_LVL1 = Decimal("500")    # спонсору 1-го уровня
BONUS_LVL2 = Decimal("250")    # спонсору 2-го уровня


def _recipient(u: User):
    """Куда писать транзакцию: master_profile для мастера, иначе сам User."""
    return u.master_profile if u.role == "Master" else u


# ────────────────────────────────────────────────────────────────
# 1. Бонус сразу после регистрации ─ ТОЛЬКО самому новичку
# ────────────────────────────────────────────────────────────────
def apply_registration_bonus(new_user: User) -> None:
    create_bonus_tx(
        _recipient(new_user),
        BONUS_SELF,
        "Приветственный бонус за регистрацию"
    )


# ────────────────────────────────────────────────────────────────
# 2. Бонус после ПЕРВОГО депозита ─ ТОЛЬКО спонсорам
# ────────────────────────────────────────────────────────────────
def apply_first_deposit_bonus(invited_user: User) -> None:
    sponsor_1 = invited_user.referrer
    if sponsor_1:
        create_bonus_tx(
            _recipient(sponsor_1),
            BONUS_LVL1,
            "Бонус 1-й линии после первого пополнения приглашённого"
        )
        sponsor_2 = sponsor_1.referrer
        if sponsor_2:
            create_bonus_tx(
                _recipient(sponsor_2),
                BONUS_LVL2,
                "Бонус 2-й линии после первого пополнения приглашённого"
            )