# utils.py

import re
from collections import defaultdict

STATUS_MAPPING = {
    'Open': 65736946,           # Новая заявка
    'In Progress': 63819782,    # Взяли в работу
    'Cancelled': 63819786,      # Отказ от ремонта
    'Completed': 142,           # Успешно реализовано
    'Free': 63819778,           # Свободная заявка
    'AwaitingClosure': 72644046,# Ожидает закрытия (новый)
    'Closed': 143,               # Закрыто (новый)
    'QualityControl': 67450158

}


MASTER_LEVEL_MAPPING = {
    'Мастер': 1,
    'Грандмастер': 2,
    'Учитель': 3
}

REVERSE_STATUS_MAPPING = {v: k for k, v in STATUS_MAPPING.items()}

def parse_nested_form_data(form_data):
    """
    Преобразует плоские ключи формы с синтаксисом вложенных полей
    в вложенный словарь.
    
    Пример:
        'leads[status][0][id]': '24753745'
        превращается в:
        {
            'leads': {
                'status': [
                    {
                        'id': 24753745,
                        'status_id': 63819778,
                        ...
                    }
                ]
            },
            'account': {
                'id': 28733683,
                'subdomain': 'servicecentru'
            }
        }
    """
    nested_data = {}
    status_pattern = re.compile(r'(\w+)\[(\w+)\]\[(\d+)\]\[(\w+)\]')
    simple_pattern = re.compile(r'(\w+)\[(\w+)\]')

    for key, value in form_data.items():
        status_match = status_pattern.match(key)
        if status_match:
            main_key, sub_key, index, field = status_match.groups()
            index = int(index)

            if main_key not in nested_data:
                nested_data[main_key] = {}

            if sub_key not in nested_data[main_key]:
                nested_data[main_key][sub_key] = []

            # Расширяем список до нужного индекса
            while len(nested_data[main_key][sub_key]) <= index:
                nested_data[main_key][sub_key].append({})

            # Преобразуем числовые поля
            if field in ['id', 'status_id', 'pipeline_id', 'old_status_id', 'old_pipeline_id']:
                try:
                    value = int(value)
                except ValueError:
                    pass  # Оставляем как строку, если не удалось преобразовать

            nested_data[main_key][sub_key][index][field] = value
        else:
            # Обрабатываем простые поля, например, account[id]
            simple_match = simple_pattern.match(key)
            if simple_match:
                main_key, field = simple_match.groups()
                if main_key not in nested_data:
                    nested_data[main_key] = {}
                nested_data[main_key][field] = value
            else:
                # Обрабатываем любые другие поля
                nested_data[key] = value

    return nested_data


from decimal import Decimal

def decimal_to_str_no_trailing_zeros(value: Decimal | None) -> str:
    """
    Преобразует Decimal в строку без лишних нулей. 
    Если число целое - без десятичных знаков, 
    иначе возвращает дробную часть.
    """
    if value is None:
        return "0"
    value = value.normalize()  # убираем trailing zeros
    if value == value.to_integral():
        # целое число
        return str(value.to_integral())
    return str(value)



from serviceapp.models import Settings

def get_amocrm_bearer_token():
    """
    Получает актуальный токен AmoCRM из базы данных.
    Если токена нет, возвращает пустую строку.
    """
    settings = Settings.objects.first()
    return settings.amocrm_bearer_token if settings else ''
