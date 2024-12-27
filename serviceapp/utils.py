# utils.py

STATUS_MAPPING = {
    'Open': 65736946,          # Новая заявка
    'In Progress': 63819782,   # Взяли в работу
    'Cancelled': 63819786,     # Отказ от ремонта
    'Completed': 142,          # Успешно реализовано
    'Free': 63819778,           # Свободная заявка
}

REVERSE_STATUS_MAPPING = {v: k for k, v in STATUS_MAPPING.items()}
