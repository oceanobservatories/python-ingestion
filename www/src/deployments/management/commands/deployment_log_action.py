from django.core.management.base import BaseCommand, CommandError
from deployments import models 
from django.conf import settings

from authtools.models import User

class Command(BaseCommand):
    help = 'Logs an action to a deployment.'
    def add_arguments(self, parser):
        parser.add_argument('deployment', type=int)
        parser.add_argument('user', type=int)
        parser.add_argument('success', type=str, choices=["True", "False"])

    def handle(self, *args, **options):
        user = User.objects.get(id=options['user'])
        deployment = models.Deployment.objects.get(id=options['deployment'])
        success = options['success']
        msg = {
            'True':   "Finished ingestion for deployment %s." % deployment.designator,
            'False':  "There was a problem with this ingestion. Check the worker logs for more details.",
            }[success]
        deployment.log_action(user, msg)
        
