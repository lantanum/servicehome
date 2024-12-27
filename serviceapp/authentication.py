# serviceapp/authentication.py

from rest_framework import authentication, exceptions
from django.conf import settings

class FixedTokenAuthentication(authentication.BaseAuthentication):
    """
    Простая аутентификация по фиксированному токену.
    Клиент должен отправить заголовок:
    Authorization: Token <your-fixed-token>
    """

    keyword = 'Token'

    def authenticate(self, request):
        auth_header = authentication.get_authorization_header(request).split()

        if not auth_header or auth_header[0].decode().lower() != self.keyword.lower():
            return None  # Не аутентифицирован

        if len(auth_header) == 1:
            msg = 'Неверный заголовок авторизации. Отсутствует токен.'
            raise exceptions.AuthenticationFailed(msg)
        elif len(auth_header) > 2:
            msg = 'Неверный заголовок авторизации. Слишком много параметров.'
            raise exceptions.AuthenticationFailed(msg)

        try:
            token = auth_header[1].decode()
        except UnicodeError:
            msg = 'Неверный токен.'
            raise exceptions.AuthenticationFailed(msg)

        if token != settings.API_ACCESS_TOKEN:
            raise exceptions.AuthenticationFailed('Неверный токен.')

        # Возвращаем фиктивного пользователя и `None` для `auth`
        return (None, None)
