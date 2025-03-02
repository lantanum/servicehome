from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
from serviceapp.models import Settings

class BearerTokenUser:
    """
    Простой объект-пользователь для аутентификации по статическому Bearer-токену.
    """
    is_authenticated = True

    def __str__(self):
        return "BearerTokenUser"

class BearerTokenAuthentication(BaseAuthentication):
    """
    Аутентификатор, который проверяет заголовок Authorization на наличие Bearer-токена,
    сравнивая его с полем service_token из модели Settings.
    """
    def authenticate(self, request):
        auth_header = request.headers.get('Authorization', '')
        if not auth_header:
            raise exceptions.AuthenticationFailed('Authorization header missing.')

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            raise exceptions.AuthenticationFailed('Invalid Authorization header format. Expected "Bearer <token>".')

        token = parts[1]

        # Извлекаем токен из базы данных (предполагается, что существует ровно одна запись в Settings)
        settings_obj = Settings.objects.first()
        if not settings_obj or not settings_obj.service_token:
            raise exceptions.AuthenticationFailed('API service token not configured.')

        if token != settings_obj.service_token:
            raise exceptions.AuthenticationFailed('Invalid token.')

        return (BearerTokenUser(), None)
