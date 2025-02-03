# users_app/amocrm_client.py

import json
import requests
import logging
from django.conf import settings
from serviceapp.utils import get_amocrm_bearer_token

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

    def search_contacts(self, phone=None, telegram_id=None):
        """
        Ищет контакты по номеру телефона и/или telegram_id.
        Возвращает список найденных контактов.
        """
        if not phone and not telegram_id:
            raise ValueError("Необходимо указать хотя бы номер телефона или telegram_id для поиска.")

        url = f"{self.base_url}/contacts"
        headers = self._get_headers()

        # Формируем условия фильтрации
        conditions = []

        if phone:
            conditions.append({
                "field": {"code": "PHONE"},  # Стандартное поле "PHONE"
                "operator": "EQUALS",
                "value": phone
            })

        if telegram_id:
            conditions.append({
                "field": {"id": settings.AMOCRM_CUSTOM_FIELD_TELEGRAM_ID},  # Кастомное поле Telegram
                "operator": "EQUALS",
                "value": telegram_id
            })

        # Определяем логику фильтрации
        if len(conditions) > 1:
            filter_obj = {
                "logic": "or",
                "conditions": conditions
            }
        else:
            filter_obj = conditions  # Передаём массив условий напрямую

        # Серилизуем фильтр в JSON-строку
        params = {
            "page": 1,
            "limit": 50,
            "filter": json.dumps(filter_obj)
        }

        # Логируем параметры запроса для отладки
        logger.debug(f"Searching contacts with params: {params}")

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            try:
                contacts = response.json()['_embedded']['contacts']
                logger.info(f"Найдено {len(contacts)} контактов в AmoCRM по заданным критериям.")
                return contacts
            except (KeyError, IndexError) as e:
                logger.error(f"Неожиданный формат ответа от AmoCRM при поиске контактов: {e}, текст ответа: {response.text}")
                return []
        else:
            # Логируем подробную информацию об ошибке
            logger.error(
                "Не удалось выполнить поиск контактов в AmoCRM. "
                "Статус: %s, Ответ: %s, "
                "Параметры запроса: %s", 
                response.status_code, response.text, params
            )
            response.raise_for_status()


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
