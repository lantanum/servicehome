import logging
from django.http import JsonResponse
from serviceapp.models import Settings

logger = logging.getLogger(__name__)

class AllowedHostsAndTokenMiddleware:
    """
    Middleware для проверки запросов:
    - Если в заголовке присутствует токен (Authorization), сравниваем его с service_token из Settings.
    - Если токена нет, проверяем, что Origin запроса входит в список разрешённых хостов.
    - Если путь начинается с /admin, то запрос пропускается без проверки.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        settings_obj = self.get_settings()
        
        request_info = {
            "path": request.path,
            "method": request.method,
            "origin": request.headers.get("Origin", "Не указан"),
            "ip": request.META.get("REMOTE_ADDR", "Неизвестен"),
        }

        if request.path.startswith('/admin'):
            logger.info(f"Админ-доступ разрешён: {request_info}")
            return self.get_response(request)

        token_in_header = request.headers.get('Authorization')
        origin = request.headers.get('Origin')

        if token_in_header:
            token = self.extract_bearer_token(token_in_header)
            if not token or token != settings_obj.service_token:
                logger.warning(f"ОТКАЗАНО: Неверный токен | {request_info}")
                return JsonResponse({"detail": "Неверный токен."}, status=403)
        else:
            # Если токена нет, проверяем Origin
            allowed_hosts = self.get_allowed_hosts(settings_obj)
            if origin and origin not in allowed_hosts:
                logger.warning(f"ОТКАЗАНО: Доступ запрещен (неразрешённый Origin) | {request_info}")
                return JsonResponse({"detail": "Доступ запрещен."}, status=403)

        logger.info(f"ДОСТУП РАЗРЕШЁН: {request_info}")
        return self.get_response(request)

    def extract_bearer_token(self, authorization_header):
        """
        Извлекает токен из заголовка Authorization: Bearer <token>.
        """
        parts = authorization_header.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1] 
        return None

    def get_settings(self):
        """
        Получает объект Settings. Если его нет, создаёт с дефолтными значениями.
        """
        settings_obj = Settings.objects.first()
        if not settings_obj:
            settings_obj = Settings.objects.create(
                service_token="",
                allowed_hosts="http://localhost, http://127.0.0.1"
            )
        return settings_obj

    def get_allowed_hosts(self, settings_obj):
        """
        Получает список разрешенных хостов из поля allowed_hosts.
        """
        if settings_obj.allowed_hosts:
            return [host.strip() for host in settings_obj.allowed_hosts.split(',')]
        return ["http://localhost", "http://127.0.0.1"]
