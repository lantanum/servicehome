from django.contrib import admin

from .models import User, ServiceRequest, Master

# Register your models here.

admin.site.register(User)
admin.site.register(ServiceRequest)
admin.site.register(Master)