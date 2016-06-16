from django.contrib import admin
from triage.models import EDEXEvent, FileEvent

@admin.register(EDEXEvent)
class EDEXEventAdmin(admin.ModelAdmin):
    pass

@admin.register(FileEvent)
class FileEventAdmin(admin.ModelAdmin):
    pass
