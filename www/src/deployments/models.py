from __future__ import unicode_literals
import sys, traceback

from django.core.urlresolvers import reverse

from django.db import models
from polymorphic.models import PolymorphicModel

from django.dispatch import receiver

from ingestion import Ingestor

from deployments import settings


DATA_SOURCE_TYPE_DEFAULTS = {
    'R': 'recovered', 
    'D': 'telemetered', 
    'X': 'telemetered',
    }

DATA_FILE_STATUS_CHOICES = (
    (c, c) for c
    in ('pending', 'ingesting', 'ingested')
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

    @classmethod
    def split_designator(cls, designator):
        reference_designator, deployment = designator.split("_")[:2]
        number = int(deployment[-5:])
        data_source_abbr = deployment[:-5]
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

    def ingest(self, annotations={}, ingest_options={}):
        options = settings.INGESTOR
        options.update(ingest_options)
        ingestor = Ingestor(**options)

        result = {
            'deployment': self,
            'user': annotations.get('user'),
            'success': True
            }

        routes = {}
        for d in self.data_groups.all():
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
                ingestor.load_queue(mask, routes, self.number)
            ingestor.ingest_from_queue(use_billiard=True)
        except:
            traceback.print_exc(file=sys.stdout)
            result['success'] = False
        return result

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

class DataFile(models.Model):
    data_group = models.ForeignKey(DataGroup)
    file_path = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=DATA_FILE_STATUS_CHOICES)

class Action(PolymorphicModel):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name="actions")
    action = models.CharField(max_length=255)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __unicode__(self):
        return u"%s: %s" % (self.user, self.action)

class DeploymentAction(Action):
    deployment = models.ForeignKey(Deployment, related_name="actions")

@receiver(models.signals.post_save, sender=DeploymentAction)
def on_created(instance, created, **kwargs):
    if created:
        print instance.user, instance.action

