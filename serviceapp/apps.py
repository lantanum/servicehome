from django.apps import AppConfig


class ServiceappConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'serviceapp'
    def ready(self):
        import serviceapp.signals
