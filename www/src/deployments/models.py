from __future__ import unicode_literals
import sys, traceback

from django.conf import settings
from django.core.urlresolvers import reverse
from django.core.files.base import ContentFile

from django.db import models
from polymorphic.models import PolymorphicModel

from django.dispatch import receiver

from ingestion import Ingestor

DATA_SOURCE_TYPE_DEFAULTS = {
    'R': 'recovered', 
    'D': 'telemetered', 
    'X': 'telemetered',
    }

DATA_FILE_STATUS_CHOICES = (
    (c, c) for c
    in ('pending', 'ingesting', 'ingested')
    )

INGESTION_STATUS_CHOICES = (
    (c, c) for c
    in ('pending', 'scheduled', 'running', 'complete')
    )

class Platform(models.Model):
    reference_designator = models.CharField(max_length=100, unique=True)

    def __unicode__(self):
        return self.reference_designator

class DataSourceType(models.Model):
    name = models.CharField(max_length=20)
    abbr = models.CharField(max_length=3, unique=True,
        verbose_name="Abbreviation")

    def __unicode__(self):
        return self.name

    class Meta:
        verbose_name = "Data Source Type"

    @classmethod
    def get_or_create_from(cls, name=None, abbr=None):
        if not any([name, abbr]):
            return None, False
        if name and abbr:
            return cls.objects.get_or_create(name=name, abbr=abbr)
        if name and not abbr:
            try:
                return cls.objects.filter(name=name)[0], False
            except IndexError:
                return cls.objects.get_or_create(name=name, abbr="".join([n[:1] for n in name.split("_")]).upper())
        if abbr and not name:
            try:
                return cls.objects.get(abbr=abbr), False
            except cls.DoesNotExist:
                return cls.objects.get_or_create(name=DATA_SOURCE_TYPE_DEFAULTS[abbr], abbr=abbr)
            except IndexError:
                return cls.objects.get_or_create(name=abbr, abbr=abbr)

class Deployment(models.Model):
    platform = models.ForeignKey(Platform)
    data_source = models.ForeignKey(DataSourceType,
        verbose_name="Data Source")
    number = models.IntegerField()
    csv_file = models.FileField(null=True, blank=True, 
        verbose_name="CSV File")

    class Meta:
        unique_together = ('platform', 'data_source', 'number')

    class AlreadyExists(Exception):
        def __init__(self, obj):
            self.message = u"A deployment for %s already exists." % obj.designator
            self.object = obj
        
        def html_link_to_object(self, text):
            return "<a href='%s'>%s</a>" % (self.object.get_absolute_url(), text)

    class BadDesignator(Exception):
        def __init__(self, designator):
            self.message = u"Can't parse the deployment's designator (%s)." % designator

    @classmethod
    def split_designator(cls, designator):
        reference_designator, deployment = [
            x for x 
            in designator.split(".")[0].split("/")[-1].split("_") 
            if x != "ingest"
            ]
        deployment = deployment.split("-")[0]
        try:
            number = int(deployment[1:6])
            data_source_abbr = deployment[0]
        except:
            raise cls.BadDesignator(designator)
        return reference_designator, data_source_abbr, number

    @classmethod
    def create_from_csv_file(cls, csv_file):
        ''' Create a new Deployment object from a FieldFile object. '''
        reference_designator, data_source_abbr, number = cls.split_designator(csv_file._name)
        platform, p_created = Platform.objects.get_or_create(reference_designator=reference_designator)
        data_source, ds_created = DataSourceType.get_or_create_from(abbr=data_source_abbr)
        try:
            deployment = cls.objects.get(platform=platform, data_source=data_source, number=number)
        except cls.DoesNotExist:
            return cls.objects.create(
                platform=platform, data_source=data_source, number=number, csv_file=csv_file)
        raise cls.AlreadyExists(deployment)

    @classmethod
    def create_or_update_from_content(cls, file_name, content):
        ''' Create a new Deployment object from a FieldFile object. '''
        reference_designator, data_source_abbr, number = cls.split_designator(file_name)
        platform, p_created = Platform.objects.get_or_create(reference_designator=reference_designator)
        data_source, ds_created = DataSourceType.get_or_create_from(abbr=data_source_abbr)
        try:
            deployment = cls.objects.get(
                platform=platform, data_source=data_source, number=number)
            created = False
        except cls.DoesNotExist:
            deployment = cls.objects.create(
                platform=platform, data_source=data_source, number=number)
            created = True
        deployment.csv_file.save(file_name, ContentFile(content))
        deployment.save()
        return deployment, created
    
    @classmethod
    def get_by_designator(cls, designator):
        reference_designator, data_source_abbr, number = cls.split_designator(designator)
        return cls.objects.get(
            platform__reference_designator=reference_designator,
            number=number, data_source__abbr=data_source_abbr)

    @property
    def designator(self):
        return u"%s_%s%05d" % (self.platform, self.data_source.abbr, self.number)

    def __unicode__(self):
        return self.designator

    def process_csv(self):
        ''' Deletes all data groups associated with this deployment and reparses the CSV to create fresh ones. '''
        for data_group in self.data_groups.all():
            data_group.delete()

        processed_csv = Ingestor.process_csv(self.csv_file.path)
        data_groups = []
        for mask, routes, deployment_number in processed_csv:
            for route in routes:
                data_source_from_route = route.pop('data_source')
                if self.data_source.name == data_source_from_route:
                    data_source_type = self.data_source
                else:
                    data_source_type, ds_created = DataSourceType.get_or_create_from(name=data_source_from_route)
                data_groups.append(
                    DataGroup.objects.get_or_create(
                        deployment=self, file_mask=mask, data_source=data_source_type, **route)[0])
        return data_groups

    def get_absolute_url(self):
        return reverse('deployments:detail', kwargs={'slug': self.designator, })

    def log_action(self, user, action):
        DeploymentAction.objects.create(deployment=self, user=user, action=action)

class DataGroup(models.Model):
    deployment = models.ForeignKey(Deployment, related_name="data_groups")
    file_mask = models.CharField(max_length=255)
    uframe_route = models.CharField(max_length=255)
    reference_designator = models.CharField(max_length=255)
    data_source = models.ForeignKey(DataSourceType, null=True, blank=True)

class Ingestion(models.Model):
    # Metadata
    deployment = models.ForeignKey(Deployment, related_name="ingestions")
    index = models.PositiveIntegerField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, 
        choices=INGESTION_STATUS_CHOICES, default="pending")

    # Options
    test_mode = models.BooleanField(default=False,
        verbose_name="Test Mode")
    force_mode = models.BooleanField(default=False,
        verbose_name="Force Mode")
    no_edex = models.BooleanField(default=False,
        verbose_name="No EDEX Check")
    health_check_enabled = models.BooleanField(default=False,
        verbose_name="Enable Health Check")
    sleep_timer = models.IntegerField(null=True, blank=True,
        verbose_name="Sleep Timer")
    max_file_age = models.IntegerField(blank=True, null=True,
        verbose_name="Max File Age")
    start_date = models.DateField(blank=True, null=True,
        verbose_name="Start Date")
    end_date = models.DateField(blank=True, null=True,
        verbose_name="End Date")
    cooldown = models.IntegerField(null=True, blank=True,
        verbose_name="EDEX Services Cooldown Timer")
    quick_look_quantity = models.IntegerField(null=True, blank=True,
        verbose_name="Quick Look Ingestion Quantity")
    edex_command = models.CharField(max_length=255, null=True, blank=True,
        verbose_name="EDEX Command")
    qpid_host = models.CharField(max_length=255, null=True, blank=True,
        verbose_name="QPID Server Host")
    qpid_port = models.IntegerField(null=True, blank=True,
        verbose_name="QPID Server Port")
    qpid_user = models.CharField(max_length=255, null=True, blank=True,
        verbose_name="QPID Server Username")
    qpid_password = models.CharField(max_length=255, null=True, blank=True,
        verbose_name="QPID Server Password")

    def get_absolute_url(self):
        return reverse(
            'deployments:ingestion_detail', 
            kwargs={
                'deployment_designator': self.deployment.designator, 
                'index': self.index,
                })

    def designator(self):
        return u"%s-%d" % (self.deployment.designator, self.index)

    def configuration_fieldsets(self):
        def get_verbose_name(s):
            return self.__class__._meta.get_field_by_name(s)[0].verbose_name
        fieldsets = (
            ('Switches', 'test_mode', 'force_mode', 'health_check_enabled', ),
            ('File Ingestion', 'sleep_timer', 'max_file_age', 'start_date', 'end_date', 'quick_look_quantity', ),
            ('QPID', 'qpid_host', 'qpid_port', 'qpid_user', 'qpid_password', ),
            ('EDEX', 'edex_command', 'cooldown', 'no_edex', ),
            )
        return [
            {'heading': fieldset[0],
            'fields': [{
                    'label': get_verbose_name(field), 
                    'value': getattr(self, field),
                    }
                for field in fieldset[1:]
                ]}
            for fieldset in fieldsets
            ]

    @property
    def options(self):
        return {
            k: v for k, v in self.__dict__.iteritems() 
            if k not in ('state', 'deployment_id', 'id', 'index', 'timestamp')
            }

    def ingest(self, annotations={}):
        ingestor = Ingestor(**self.options)

        result = {
            'deployment': self.deployment,
            'user': annotations.get('user'),
            'success': True
            }

        routes = {}
        for d in self.deployment.data_groups.all():
            parameters = {
                'uframe_route': d.uframe_route, 
                'reference_designator': d.reference_designator, 
                'data_source': d.data_source.name,
                }
            if d.file_mask in routes.keys():
                routes[d.file_mask].append(parameters)
            else:
                routes[d.file_mask] = [parameters]
        data_groups = [(mask, routes[mask]) for mask in routes]

        try:
            for mask, routes in data_groups:
                ingestor.load_queue(mask, routes, self.deployment.number)
            
            # Analyze the queue.
            for batch in ingestor.queue:
                for file_path, routes in batch['files']:
                    DataFile.objects.get_or_create(file_path=file_path, status="pending")
            
            self.status = 'running'
            ingestor.ingest_from_queue(use_billiard=False)
            self.status = 'complete'
        except:
            traceback.print_exc(file=sys.stdout)
            result['success'] = False
        return result

    def log_action(self, user, action):
        IngestionAction.objects.create(ingestion=self, user=user, action=action)

class DataFile(models.Model):
    file_path = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=DATA_FILE_STATUS_CHOICES)
    ingestion = models.ForeignKey(Ingestion)

class Action(PolymorphicModel):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name="actions")
    action = models.CharField(max_length=255)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __unicode__(self):
        return u"%s: %s" % (self.user.get_full_name(), self.action)

class DeploymentAction(Action):
    deployment = models.ForeignKey(Deployment, related_name="actions")

class IngestionAction(Action):
    ingestion = models.ForeignKey(Ingestion, related_name="actions")

class DataFileAction(Action):
    data_file = models.ForeignKey(DataFile, related_name="actions")

@receiver(models.signals.post_save, sender=DeploymentAction)
def on_created(instance, created, **kwargs):
    if created:
        print instance.user, instance.action

@receiver(models.signals.post_save, sender=Ingestion)
def on_created(instance, created, **kwargs):
    if created:
        instance.index = instance.deployment.ingestions.count()
        instance.save()
