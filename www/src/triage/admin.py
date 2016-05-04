from django.contrib import admin
from triage.models import EDEXEvent

@admin.register(EDEXEvent)
class EDEXEventAdmin(admin.ModelAdmin):
    pass
