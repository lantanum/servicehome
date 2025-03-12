from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
from serviceapp.models import Settings

class BearerTokenUser:
    is_authenticated = True

    def __str__(self):
        return "BearerTokenUser"

class BearerTokenAuthentication(BaseAuthentication):
    def authenticate(self, request):
        # 1. Разрешаем AmoCRM без аутентификации
        amocrm_allowed_domains = ["https://servicecentru.amocrm.ru"]
        origin = request.headers.get("Origin", "")
        referer = request.headers.get("Referer", "")

        if origin in amocrm_allowed_domains or referer.startswith("https://servicecentru.amocrm.ru"):
            return None  # Пропускаем аутентификацию для AmoCRM

        # 2. Если запрос не от AmoCRM, проверяем Bearer-токен
        auth_header = request.headers.get("Authorization", "")
        if not auth_header:
            raise exceptions.AuthenticationFailed("Authorization header missing.")

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise exceptions.AuthenticationFailed('Invalid Authorization header format. Expected "Bearer <token>".')

        token = parts[1]

        settings_obj = Settings.objects.first()
        if not settings_obj or not settings_obj.service_token:
            raise exceptions.AuthenticationFailed("API service token not configured.")

        if token != settings_obj.service_token:
            raise exceptions.AuthenticationFailed("Invalid token.")

        return (BearerTokenUser(), None)
