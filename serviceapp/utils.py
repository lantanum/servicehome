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
    1: "Мастер",
    2: "Грандмастер",
    3: "Учитель"
}


REVERSE_STATUS_MAPPING = {v: k for k, v in STATUS_MAPPING.items()}

def parse_nested_form_data(form_data):
    """
    Универсальное разбиение: ищем все фрагменты между '[' и ']' и первый фрагмент до них.
    Пример:
      leads[status][0][custom_fields_values][0][field_id]
    разложится в список:
      ["leads", "status", "0", "custom_fields_values", "0", "field_id"]
    И дальше мы постепенно строим вложенный словарь.
    """

    nested_dict = {}

    for full_key, value in form_data.items():
        # Вытащим все куски, где \w включает буквы, цифры и _, и
        # также отдельно возьмём ведущую часть до первой '['
        # re.findall(r"\w+", key) обычно вытащит все подряд (включая подчеркивания).
        # Например, 'leads[status][0][custom_fields_values][0][field_id]' -> ['leads','status','0','custom_fields_values','0','field_id']
        path = re.findall(r"\w+", full_key)
        if not path:
            continue

        # Теперь идём по path и создаём структуры в виде словарей / списков
        current_level = nested_dict
        for i, key_part in enumerate(path):
            is_last = (i == len(path) - 1)

            # Пытаемся понять, это индекс в списке (число) или обычный ключ словаря
            try:
                idx = int(key_part)
                # Это индекс => значит, current_level должен быть списком
                # если current_level ещё не список, превратим в список
                if not isinstance(current_level, list):
                    # Преобразуем current_level в список внутри текущего ключа.
                    # Но чтобы это было корректно, нужно знать,
                    # что предыдущая точка в path тоже являлась числом.
                    # Часто перед этим идёт название ключа (который превращаем в список).
                    # Однако, если логика simpler — можно делать "named lists", например:
                    # nested_dict["some_list"] = [{}, {}, ...]
                    pass
                # Увеличиваем длину списка до нужного индекса
                while len(current_level) <= idx:
                    current_level.append({})
                if is_last:
                    current_level[idx] = value
                else:
                    # если ещё не последний уровень, нужно идти глубже
                    if not isinstance(current_level[idx], (dict, list)):
                        current_level[idx] = {}
                    current_level = current_level[idx]
            except ValueError:
                # Не число => работаем как со словарём
                if is_last:
                    current_level[key_part] = value
                else:
                    if key_part not in current_level:
                        # Определяем, создаём ли здесь dict или list
                        # чаще всего dict, пока не встретим число
                        current_level[key_part] = {}
                    current_level = current_level[key_part]

    return nested_dict

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
