from django.contrib import admin
from deployments import models

@admin.register(models.Deployment)
class DeploymentAdmin(admin.ModelAdmin):
    list_display = ('platform', 'number', 'data_source')

@admin.register(models.DataSourceType)
class DataSourceTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'abbr')

@admin.register(models.Ingestion)
class IngestionAdmin(admin.ModelAdmin):
    list_display = ('index', 'timestamp', 'deployment')

