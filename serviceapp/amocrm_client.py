# users_app/amocrm_client.py

import json
import re
import requests
import logging
from django.conf import settings
from serviceapp.utils import get_amocrm_bearer_token
import time
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class AmoCRMClient:
    def __init__(self):
        """
        Предполагаем, что токен задан в settings.py или в переменных окружения.
        """
        self.bearer_token = get_amocrm_bearer_token()  # В settings.py хранится ваш Bearer-токен
        self.subdomain = settings.AMOCRM_SUBDOMAIN        # Например, "servicecentru"
        self.base_url = f'https://{self.subdomain}.amocrm.ru/api/v4'  # или api-b.amocrm.ru, если нужно

    def _get_headers(self):
        """
        Формируем заголовки для запросов к API с использованием Bearer-токена.
        """
        return {
            'Authorization': f'Bearer {self.bearer_token}',
            'Content-Type': 'application/json'
        }

    def create_contact(self, contact_data):
        """
        Создаёт контакт (физ. лицо) в AmoCRM. В случае успеха возвращает dict с данными контакта,
        при ошибке — логирует и выбрасывает исключение.
        """
        url = f'{self.base_url}/contacts'
        headers = self._get_headers()

        # Для отладки можно залогировать, какие данные уходим отправлять
        logger.debug("Creating contact in AmoCRM. URL: %s, Data: %s", url, contact_data)

        response = requests.post(url, json=[contact_data], headers=headers)

        # При успехе 200 или 201
        if response.status_code in [200, 201]:
            try:
                contact = response.json()['_embedded']['contacts'][0]
                logger.info("Contact created in AmoCRM with ID: %s", contact['id'])
                return contact
            except (KeyError, IndexError) as e:
                logger.error("Unexpected response format from AmoCRM: %s, text=%s", e, response.text)
                raise
        else:
            # Логируем подробную информацию об ошибке
            logger.error(
                "Failed to create contact in AmoCRM. "
                "Status code: %s, Response text: %s, "
                "Tried data: %s", 
                response.status_code, response.text, contact_data
            )
            response.raise_for_status()

    def get_contact_by_id(self, contact_id):
        """
        Получение контакта по ID из AmoCRM.
        """
        url = f'{self.base_url}/contacts/{contact_id}'
        headers = self._get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get contact from AmoCRM: {response.text}")
            response.raise_for_status()

    def create_lead(self, lead_data):
        """
        Создаёт лид (Lead) в AmoCRM v4.
        lead_data - словарь вида:
        {
          "name": "...",
          "status_id": 63819782,
          "pipeline_id": 7734406,
          "_embedded": { "contacts": [{ "id": ... }] },
          "custom_fields_values": [...]
        }

        В AmoCRM API v4 обычно отправляем массив [ lead_data ].
        """
        url = f'{self.base_url}/leads'
        headers = self._get_headers()
        response = requests.post(url, json=[lead_data], headers=headers)
        if response.status_code in [200, 201]:
            # response.json()['_embedded']['leads'][0]
            lead = response.json()['_embedded']['leads'][0]
            logger.info(f"Lead created in AmoCRM with ID={lead['id']}")
            return lead
        else:
            logger.error("Failed to create lead in AmoCRM. status=%s, text=%s, data=%s",
                         response.status_code, response.text, lead_data)
            response.raise_for_status()
    # Можно добавить методы update_contact, search_contact и т.д.
    def update_contact(self, contact_id, contact_data):
        """
        Обновляет (PATCH) контакт {contact_id}.
        Или можно POST /api/v4/contacts c [{id:..., ...}]
        """
        url = f"{self.base_url}/contacts/{contact_id}"
        headers = self._get_headers()
        # В API v4 часто используют mass update. 
        # Но здесь покажем PATCH для одного контакта:
        response = requests.patch(url, json=contact_data, headers=headers)
        if response.status_code in [200, 201]:
            contact = response.json()
            logger.info(f"Contact updated in AmoCRM with ID: {contact['id']}")
            return contact
        else:
            logger.error("Failed to update contact in AmoCRM. "
                         "Status=%s, Resp=%s, Data=%s",
                         response.status_code, response.text, contact_data)
            response.raise_for_status()

    def update_lead(self, lead_id, data):
        """
        Обновление информации о лиде.
        """
        url = f"{self.base_url}/leads/{lead_id}"
        # Берём заголовки через вызов метода _get_headers()
        headers = self._get_headers()
        response = requests.patch(url, headers=headers, json=data)
        if response.status_code not in (200, 204):
            logger.error(f"Failed to update lead {lead_id}: {response.text}")
            response.raise_for_status()
        logger.info(f"Lead {lead_id} updated successfully")
        return response.status_code

    def get_lead(self, lead_id):
        """
        Получение информации о лиде.
        """
        url = f"{self.base_url}/leads/{lead_id}"
        headers = self._get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch lead {lead_id}: {response.text}")
            response.raise_for_status()
        return response.json()

    def search_contacts(
        self,
        phone: Optional[str] = None,
        telegram_id: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Ищет контакты в AmoCRM v4.

        ▸ phone – ищем по стандартным полям через «query=…»
        ▸ telegram_id – ищем по кастом-полю Telegram-ID (ID = 744499)

        Возвращает список json-объектов контактов (как даёт AmoCRM в `_embedded.contacts`).

        Пример успешного URL ↓
        https://mydomain.amocrm.ru/api/v4/contacts
            ?page=1
            &limit=50
            &query=79533550222
            &filter[custom_fields_values][744499]=123456789
        """
        if phone is None and telegram_id is None:
            raise ValueError("Нужно передать хотя бы phone или telegram_id")

        url = f"{self.base_url}/contacts"
        params: Dict[str, Any] = {"page": page, "limit": limit}

        # ───── телефон ────────────────────────────────────────────────
        if phone:
            # Очищаем всё, кроме цифр, чтобы «+7 (953)…» → «7953…»
            digits_only = re.sub(r"\D+", "", phone)
            params["query"] = digits_only

        # ───── Telegram-ID ────────────────────────────────────────────
        if telegram_id:
            # AmoCRM не возражает против строки; int() → str() на всякий случай
            params["filter[custom_fields_values][744499]"] = str(int(telegram_id))

        # ───── запрос ────────────────────────────────────────────────
        response = requests.get(
            url,
            headers=self._get_headers(),     # <-- ваш метод авторизации
            params=params,
            timeout=15,
        )
        response.raise_for_status()          # бросит HTTPError, если не 2xx

        return response.json().get("_embedded", {}).get("contacts", [])


    

    def attach_contact_to_lead(self, lead_id, contact_id):
        # Получаем лид
        lead = self.get_lead(lead_id)
        existing_contacts = lead.get('_embedded', {}).get('contacts', [])
    
        # Проверяем, нет ли уже нужного контакта
        if any(contact['id'] == contact_id for contact in existing_contacts):
            logger.info(f"Контакт {contact_id} уже прикреплён к лиду {lead_id}")
        else:
            # Формируем новый список, добавляя контакт
            updated_contacts = existing_contacts + [{'id': contact_id}]
    
            # Обновляем лид
            self.update_lead(lead_id, {'_embedded': {'contacts': updated_contacts}})
            logger.info(f"Контакт {contact_id} успешно прикреплён к лиду {lead_id}")

    def get_lead_links(self, lead_id):
        """
        Получает список связанных объектов для лида (ищем контакты).
        """
        url = f"{self.base_url}/leads/{lead_id}/links"
        headers = self._get_headers()
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Не удалось получить ссылки лида {lead_id}: {response.text}")
            return {}
    # users_app/amocrm_client.py

    def parse_contact_data(contact_json: dict) -> dict:
        """
        Извлекает телефон, имя, Telegram ID и другие нужные поля
        из ответа AmoCRM /api/v4/contacts/{contact_id}.
        
        Пример входных данных см. в теле вопроса:
        {
            "id": 29504626,
            "name": "Дмитрий Тестер",
            ...
            "custom_fields_values": [
                {
                    "field_code": "PHONE",
                    "values": [{ "value": "+79057890223" }]
                },
                {
                    "field_id": 744499,
                    "field_name": "Телеграмм ID",
                    "values": [{ "value": "844860156" }]
                },
                ...
            ]
        }
    
        Возвращаем dict, например:
        {
            "amo_crm_contact_id": 29504626,
            "name": "Дмитрий Тестер",
            "phone": "+79057890223",
            "telegram_id": "844860156",
            "role": "Client",   # если есть в полях
            "city_name": "Тестогород"  # если есть в полях
        }
        """
        contact_id = contact_json.get("id")
        name = contact_json.get("name", "Без имени")
    
        phone = None
        telegram_id = None
        role = None
        city_name = None
    
        custom_fields = contact_json.get("custom_fields_values", [])
        for cf in custom_fields:
            field_code = cf.get("field_code")  # Может быть "PHONE"
            field_id = cf.get("field_id")      # Например, 744499 для Telegram
            values = cf.get("values", [])
    
            if field_code == "PHONE" and values:
                phone = values[0].get("value")  # Берём первый телефон
    
            # Ваш кастомный ID для Telegram = 744499
            if field_id == 744499 and values:
                telegram_id = values[0].get("value")
    
            # Ваш кастомный ID для "Роль" = 744523
            if field_id == 744523 and values:
                role = values[0].get("value")
    
            # Ваш кастомный ID для "Город" = 744219 (судя по примеру)
            if field_id == 744219 and values:
                city_name = values[0].get("value")
    
        return {
            "amo_crm_contact_id": contact_id,
            "name": name,
            "phone": phone,
            "telegram_id": telegram_id,
            "role": role,
            "city_name": city_name,
        }
    def list_leads(
        self, *,
        page: int = 1,
        limit: int = 250,
        with_: str | None = None,
        created_from_ts: int | None = None,      # <-- новое
        extra: dict | None = None
    ) -> list[dict]:
        """
        GET /api/v4/leads
        Возвращает список лидов.

        created_from_ts — UNIX-время (секунды) «c какого момента брать».
                          Будет передано как filter[created_at][from].
        """
        url = f"{self.base_url}/leads"
        params = {"page": page, "limit": limit}

        if with_:
            params["with"] = with_

        if created_from_ts:
            params["filter[created_at][from]"] = created_from_ts

        if extra:
            params.update(extra)

        r = requests.get(url, headers=self._get_headers(), params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("_embedded", {}).get("leads", [])

