from django.contrib import admin

from .models import EquipmentType, ServiceType, Transaction, User, ServiceRequest, Master, Settings

# Register your models here.

admin.site.register(User)
admin.site.register(ServiceRequest)
admin.site.register(Master)
admin.site.register(Transaction)

@admin.register(ServiceType)
class ServiceTypeAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    search_fields = ['name']

@admin.register(EquipmentType)
class EquipmentTypeAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    search_fields = ['name']


@admin.register(Settings)
class SettingsAdmin(admin.ModelAdmin):
    list_display = ('comission',)