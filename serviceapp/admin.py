from django.contrib import admin

from .models import EquipmentType, InteractionLog, ReferralLink, ServiceType, Transaction, User, ServiceRequest, Master, Settings, WorkOutcome

# Register your models here.

admin.site.register(User)
admin.site.register(ServiceRequest)
admin.site.register(Master)
admin.site.register(Transaction)
admin.site.register(ReferralLink)
admin.site.register(Settings)
admin.site.register(InteractionLog)
admin.site.register(WorkOutcome)

@admin.register(ServiceType)
class ServiceTypeAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    search_fields = ['name']

@admin.register(EquipmentType)
class EquipmentTypeAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    search_fields = ['name']

