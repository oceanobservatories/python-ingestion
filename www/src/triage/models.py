from __future__ import unicode_literals

from django.db import models

from triage import definitions

class LogEvent(models.Model):
    type = models.CharField(max_length=255, choices=definitions.LOG_EVENT_TYPES)
    level = models.CharField(max_length=255, choices=definitions.LOG_EVENT_LEVELS)
    timestamp = models.DateTimeField()
    route = models.CharField(max_length=255)
    filename = models.CharField(max_length=255)
    subsite = models.CharField(max_length=255, blank=True, null=True)
    node = models.CharField(max_length=255, blank=True, null=True)
    sensor = models.CharField(max_length=255, blank=True, null=True)
    method = models.CharField(max_length=255, blank=True, null=True)
    uuid = models.CharField(max_length=255, blank=True, null=True)
    deployment = models.IntegerField(blank=True, null=True)
    parser_name = models.CharField(max_length=255, blank=True, null=True)
    parser_version = models.CharField(max_length=255, blank=True, null=True)
    particle_count = models.IntegerField(blank=True, null=True)
    error_details = models.TextField(blank=True, null=True)
