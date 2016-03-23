from django.contrib import admin
from deployments import models

@admin.register(models.Deployment)
class DeploymentAdmin(admin.ModelAdmin):
    pass

@admin.register(models.DataSourceType)
class DataSourceTypeAdmin(admin.ModelAdmin):
    list_display = ('name', 'abbr')

