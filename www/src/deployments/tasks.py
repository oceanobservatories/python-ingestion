from celery import shared_task
from celery.signals import task_success, after_task_publish

from django.core import management
from deployments.management.commands import deployment_log_action

@shared_task
def ingest(deployment, **kwargs):
    return deployment.ingest(**kwargs)

@task_success.connect
def task_success_handler(sender=None, result=None, **kwargs):
    if sender.name == "deployments.tasks.ingest":    
        # Create a DeploymentAction on the completion of a deployment ingestion.

        deployment  = result.get('deployment', None)
        user        = result.get('user', None)
        success     = result.get('success', False)

        management.call_command('deployment_log_action', str(deployment.id), str(user.id), str(success))
