from django.core.management.base import BaseCommand, CommandError
from deployments import models, tasks
from django.conf import settings

from authtools.models import User

class Command(BaseCommand):
    help = 'Ingests from a stored ingestion.'
    def add_arguments(self, parser):
        parser.add_argument('deployment', type=str)
        parser.add_argument('index', type=int)
        parser.add_argument('-c', '--use_celery', action='store_true',
            help="Use celery to ingest.")

    def handle(self, *args, **options):
        ingestion = models.Ingestion.objects.get(
            deployment=models.Deployment.get_by_designator(options['deployment']),
            index=options['index'])
        if options['use_celery']:
            tasks.ingest.delay(ingestion, annotations={'user': None, })
        else:
            ingestion.ingest(annotations={'user': None, })
