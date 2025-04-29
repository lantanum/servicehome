# serviceapp/business.py
from __future__ import annotations

"""
Мини-версия business.py: СОХРАНИТЬ / ОБНОВИТЬ клиента, мастера и заявку.

* Обрабатываем ТОЛЬКО лиды, у которых уже есть связанный контакт AmoCRM.  
* Никаких комиссий, рассылок, расчётов уровней и прочей тяжёлой логики.  
* Мастер цепляется к заявке, если его телефон или telegram-id указан
  в кастом-полях лида («Тел мастера» / «Telegram ID мастера»).
"""

import logging
from decimal import Decimal
from typing import Optional, Tuple

from django.db.models import Q

from serviceapp.amocrm_client import AmoCRMClient
from serviceapp.models import User, Master, ServiceRequest, WorkOutcome
from serviceapp.utils import REVERSE_STATUS_MAPPING

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
# 1.  Карта кастом-полей → ServiceRequest
# ────────────────────────────────────────────────────────────────
FIELD_ID_TO_MODEL_FIELD: dict[int, str] = {
    743839: "service_name",
    240631: "equipment_type",
    240637: "equipment_model",
    240635: "equipment_brand",
    240623: "city_name",
    743447: "address",
    748437: "crm_operator_comment",
    725136: "description",
    748771: "quality_rating",
    748773: "competence_rating",
    748775: "recommendation_rating",
}

# ID кастом-полей AmoCRM с данными мастера
CF_MASTER_PHONE    = 743669   # «Тел мастера»
CF_MASTER_TELEGRAM = 745549   # «Telegram ID мастера»

# ────────────────────────────────────────────────────────────────
# 2.  Работа с клиентом (User)
# ────────────────────────────────────────────────────────────────
def _update_user(user: User, **fields):
    """
    Обновляет только изменившиеся поля и сохраняет.
    """
    dirty: list[str] = []
    for k, v in fields.items():
        if v is not None and getattr(user, k) != v:
            setattr(user, k, v)
            dirty.append(k)
    if dirty:
        user.save(update_fields=dirty)
        logger.info("Пользователь %s обновлён: %s", user.id, dirty)


def find_or_update_user_from_amo(parsed: dict) -> User:
    """
    На входе — разобранный контакт AmoCRM (phone, telegram_id, name, …).
    Возвращает существующего User либо создаёт нового.
    """
    cid   = parsed.get("amo_crm_contact_id")
    phone = parsed.get("phone")
    tg_id = parsed.get("telegram_id")
    name  = parsed.get("name") or "Новый клиент"
    city  = parsed.get("city_name")

    # 1️⃣ по contact_id
    user = User.objects.filter(amo_crm_contact_id=cid).first()
    if user:
        logger.info("Контакт %s уже есть — обновляю", cid)
        _update_user(user, name=name, phone=phone, telegram_id=tg_id, city_name=city)
        return user

    # 2️⃣ по телефону
    if phone and (user := User.objects.filter(phone=phone).first()):
        logger.info("Контакт %s найден по телефону — обновляю", cid)
        _update_user(user, name=name, telegram_id=tg_id, city_name=city)
        user.amo_crm_contact_id = cid
        user.save(update_fields=["amo_crm_contact_id"])
        return user

    # 3️⃣ создаём
    logger.info("Создаю нового клиента для контакта %s", cid)
    return User.objects.create(
        name=name,
        phone=phone,
        telegram_id=tg_id,
        amo_crm_contact_id=cid,
        role="Client",
        city_name=city,
    )


def find_or_create_user_from_lead_links(
    lead_id: int,
    lead_full: dict,
    client: Optional[AmoCRMClient] = None,
) -> Optional[User]:
    """
    ▸ Ищем ссылку «лид → контакт».  
    ▸ Если контакт есть — вернём/создадим User.  
    ▸ Если контакта нет — None (лид будет пропущен).
    """
    client = client or AmoCRMClient()
    links  = client.get_lead_links(lead_id).get("_embedded", {}).get("links", [])
    contact_id = next(
        (lnk["to_entity_id"] for lnk in links if lnk["to_entity_type"] == "contacts"),
        None,
    )

    if not contact_id:
        logger.info("Лид %s пропущен: нет связанного контакта", lead_id)
        return None

    parsed = AmoCRMClient.parse_contact_data(client.get_contact_by_id(contact_id))
    return find_or_update_user_from_amo(parsed)

# ────────────────────────────────────────────────────────────────
# 3.  Кастом-поля и WorkOutcome
# ────────────────────────────────────────────────────────────────
def update_custom_fields(sr: ServiceRequest, lead_full: dict) -> None:
    dirty: list[str] = []
    for cf in lead_full.get("custom_fields_values", []):
        if not cf.get("values"):
            continue
        model_field = FIELD_ID_TO_MODEL_FIELD.get(cf.get("field_id"))
        if model_field and getattr(sr, model_field) != cf["values"][0]["value"]:
            setattr(sr, model_field, cf["values"][0]["value"])
            dirty.append(model_field)

    if dirty:
        sr.save(update_fields=dirty)
        logger.info("SR %s: кастом-поля обновлены %s", sr.id, dirty)


def set_work_outcome(sr: ServiceRequest, lead_full: dict) -> None:
    for cf in lead_full.get("custom_fields_values", []):
        if cf.get("field_id") == 745353 and cf.get("values"):
            outcome_name = cf["values"][0]["value"]
            outcome = WorkOutcome.objects.filter(outcome_name=outcome_name).first()
            if outcome and sr.work_outcome_id != outcome.id:
                sr.work_outcome = outcome
                sr.save(update_fields=["work_outcome"])
                logger.info("SR %s: WorkOutcome = %s", sr.id, outcome_name)
            break

# ────────────────────────────────────────────────────────────────
# 4.  Привязка мастера к заявке
# ────────────────────────────────────────────────────────────────
def assign_master(sr: ServiceRequest, lead_full: dict) -> None:
    """
    Вытаскиваем из кастом-полей телефон / telegram-id мастера,
    ищем Master и записываем в sr.master.
    """
    phone = tg_id = None
    for cf in lead_full.get("custom_fields_values", []):
        fid = cf.get("field_id")
        if fid == CF_MASTER_PHONE and cf.get("values"):
            phone = cf["values"][0]["value"]
        if fid == CF_MASTER_TELEGRAM and cf.get("values"):
            tg_id = str(cf["values"][0]["value"])

    if not phone and not tg_id:
        return  # данных мастера нет

    master = (
        Master.objects.select_related("user")
        .filter(Q(user__phone=phone) | Q(user__telegram_id=tg_id))
        .first()
    )

    if master and sr.master_id != master.id:
        sr.master = master
        sr.save(update_fields=["master"])
        logger.info(
            "SR %s: привязал мастера id=%s (phone=%s, tg_id=%s)",
            sr.id,
            master.id,
            phone,
            tg_id,
        )

# ────────────────────────────────────────────────────────────────
# 5.  Сохранение лида
# ────────────────────────────────────────────────────────────────
def save_lead(
    lead_short: dict,
    client: Optional[AmoCRMClient] = None,
) -> Tuple[Optional[ServiceRequest], bool]:
    """
    Создаёт/обновляет ServiceRequest.

    Возвращает:
        (<ServiceRequest>, created)     – если контакт найден,
        (None, False)                   – если у лида нет контакта.
    """
    client      = client or AmoCRMClient()
    lead_id     = lead_short["id"]
    status_id   = lead_short["status_id"]
    status_name = REVERSE_STATUS_MAPPING.get(status_id, "Open")

    lead_full = client.get_lead(lead_id)

    # ---------- клиент ----------
    user = find_or_create_user_from_lead_links(lead_id, lead_full, client)
    if user is None:
        return None, False              # лид пропускаем

    # ---------- заявка ----------
    sr, created = ServiceRequest.objects.update_or_create(
        amo_crm_lead_id=lead_id,
        defaults={
            "client": user,
            "status": status_name,
            "amo_status_code": status_id,
            "price": Decimal(lead_short.get("price") or 0),
        },
    )

    # ---------- мастер ----------
    assign_master(sr, lead_full)

    # ---------- прочие поля ----------
    update_custom_fields(sr, lead_full)
    set_work_outcome(sr, lead_full)

    logger.info("%s заявку %s", "Создал" if created else "Обновил", lead_id)
    return sr, created
