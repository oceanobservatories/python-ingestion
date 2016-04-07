from django.conf.urls import url
from django.contrib.auth.decorators import login_required, permission_required

from . import views

urlpatterns = [
    url(r'^$', login_required(views.DeploymentListView.as_view()), name="list"),
    url(r'^create/$', login_required(views.DeploymentCreateView.as_view()), name="create"),
    url(r'^detail/(?P<slug>[-\w]+)/$', login_required(views.DeploymentDetailView.as_view()), name='detail'),
    ]
