from django.http import JsonResponse
from serviceapp.models import Settings

class AllowedHostsAndTokenMiddleware:
    """
    Middleware для проверки запросов:
    - Если в заголовке присутствует токен (Authorization), сравниваем его с service_token из Settings.
    - Если токена нет, проверяем, что Origin запроса входит в список разрешённых хостов.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        settings_obj = self.get_settings()
        if request.path.startswith('/admin'):
            return self.get_response(request)

        token_in_header = request.headers.get('Authorization')
        origin = request.headers.get('Origin')

        if token_in_header:
            # Если токен присутствует, сравниваем его с service_token
            if token_in_header != settings_obj.service_token:
                return JsonResponse({"detail": "Неверный токен."}, status=403)
        else:
            # Если токена нет, проверяем Origin
            allowed_hosts = self.get_allowed_hosts(settings_obj)
            if origin and origin not in allowed_hosts:
                return JsonResponse({"detail": "Доступ запрещен."}, status=403)

        return self.get_response(request)

    def get_settings(self):
        """
        Получает объект Settings. Если его нет, можно вернуть объект с дефолтными значениями.
        """
        settings_obj = Settings.objects.first()
        if not settings_obj:
            # Если настроек нет, можно создать настройки с пустым токеном и дефолтными allowed_hosts.
            # (Либо возвращать объект с дефолтными значениями, в зависимости от логики вашего приложения)
            settings_obj = Settings.objects.create(
                service_token="",
                allowed_hosts="http://localhost, http://127.0.0.1"
            )
        return settings_obj

    def get_allowed_hosts(self, settings_obj):
        """
        Получает список разрешенных хостов из поля allowed_hosts.
        Предполагается, что это строка с доменами, разделёнными запятыми.
        """
        if hasattr(settings_obj, 'allowed_hosts') and settings_obj.allowed_hosts:
            # Разбиваем по запятой и убираем лишние пробелы
            return [host.strip() for host in settings_obj.allowed_hosts.split(',')]
        # Значения по умолчанию, если поле не задано
        return ["http://localhost", "http://127.0.0.1"]
