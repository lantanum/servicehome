from decimal import Decimal
from serviceapp.models import User
from serviceapp.utils import create_bonus_tx


def _recipient(u):
    """
    Возвращает объект-получатель для bonus_tx:
      • master_profile — если u.role == 'Master'
      • сам User       — если клиент
    """
    return u.master_profile if u.role == "Master" else u


def apply_registration_referral_bonus(new_user: User) -> None:
    """
    • самому новичку             — 500  
    • спонсору (1-я линия)       — 500  
    • «дедушке»  (2-я линия)     — 250  
    Суммы при желании меняйте.
    """
    bonus_self, bonus_lvl1, bonus_lvl2 = map(Decimal, ("500", "500", "250"))

    sponsor_1 = new_user.referrer
    sponsor_2 = sponsor_1.referrer if sponsor_1 else None

    # новичок
    create_bonus_tx(_recipient(new_user), bonus_self, "Приветственный бонус")

    # первая линия
    if sponsor_1:
        create_bonus_tx(
            _recipient(sponsor_1),
            bonus_lvl1,
            "Бонус 1-й линии за приглашённого пользователя"
        )

    # вторая линия
    if sponsor_2:
        create_bonus_tx(
            _recipient(sponsor_2),
            bonus_lvl2,
            "Бонус 2-й линии за приглашённого пользователя"
        )
