# users_app/amocrm_client.py

import requests
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class AmoCRMClient:
    def __init__(self):
        """
        Предполагаем, что токен задан в settings.py или в переменных окружения.
        """
        self.bearer_token = settings.AMOCRM_BEARER_TOKEN  # В settings.py хранится ваш Bearer-токен
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
            
    def search_contacts(self, phone=None, telegram_id=None):
        """
        Ищет контакты по номеру телефона и/или telegram_id.
        Возвращает список найденных контактов.
        """
        if not phone and not telegram_id:
            raise ValueError("Необходимо указать хотя бы номер телефона или telegram_id для поиска.")

        url = f"{self.base_url}/contacts"
        headers = self._get_headers()

        # Формируем параметры фильтрации
        params = {
            "page": 1,
            "limit": 50  # Настройте лимит по необходимости
        }

        # Строим фильтр
        filters = []
        if phone:
            filters.append({
                "field": "PHONE",
                "operator": "EQUALS",
                "value": phone
            })
        if telegram_id:
            filters.append({
                "field": settings.AMOCRM_CUSTOM_FIELD_TELEGRAM_ID,  # Предполагается, что это ID кастомного поля Telegram
                "operator": "EQUALS",
                "value": telegram_id
            })

        if filters:
            params['filter'] = filters

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
            logger.error(f"Не удалось выполнить поиск контактов в AmoCRM. Статус: {response.status_code}, Ответ: {response.text}")
            response.raise_for_status()