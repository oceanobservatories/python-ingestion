from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.DeploymentListView.as_view(), name="list"),
    url(r'^create/$', views.DeploymentCreateView.as_view(), name="create"),
    url(r'^detail/(?P<slug>[-\w]+)/$', views.DeploymentDetailView.as_view(), name='detail'),
    ]
