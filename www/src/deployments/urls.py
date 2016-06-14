from django.conf.urls import url
from django.contrib.auth.decorators import login_required, permission_required

from . import views

urlpatterns = [

    # Deployments
    url(r'^$', 
        login_required(views.DeploymentListView.as_view()), name="list"),
    url(r'^create/$', 
        login_required(views.DeploymentCreateView.as_view()), name="create"),
    url(r'^github_import/$', 
        login_required(views.DeploymentGithubImportView.as_view()), name="github_import"),
    url(r'^detail/(?P<slug>[-\w]+)/$', 
        login_required(views.DeploymentDetailView.as_view()), name='detail'),

    # Ingestions
    url(r'^detail/(?P<deployment_designator>[-\w]+)/new-ingestion$', 
        login_required(views.IngestionCreateView.as_view()), name='ingestion_create'),
    url(r'^detail/(?P<deployment_designator>[-\w]+)/ingestions/(?P<index>[-\w]+)$', 
        login_required(views.IngestionDetailView.as_view()), name='ingestion_detail'),
    ]
