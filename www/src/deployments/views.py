from django.contrib import messages
from django.http import Http404, HttpResponseRedirect
from django.shortcuts import render
from django.views import generic 

from deployments.models import Deployment, Ingestion
from deployments.forms import DeploymentCreateFromCSVForm, IngestionForm
from deployments import tasks
from deployments.settings import INGESTOR_OPTIONS

class DeploymentListView(generic.ListView):
    model = Deployment

    def get_queryset(self):
        filters = {
            'platform__reference_designator': self.request.GET.get('platform', None), 
            'number': self.request.GET.get('number', None), 
            'data_source__name': self.request.GET.get('data_source', None), 
            'data_source__abbr': self.request.GET.get('data_source_abbr', None), 
            }
        filters = {f: filters[f] for f in filters if filters[f]}
        return Deployment.objects.filter(**filters)

class DeploymentDetailView(generic.DetailView):
    model = Deployment

    def get_object(self):
        try:
            return Deployment.get_by_designator(self.kwargs.pop('slug', None))
        except:
            raise Http404(u"No deployment found.")

    def post(self, request, *args, **kwargs):
        self.object = self.get_object()
        if "_process_csv" in request.POST:
            self.object.process_csv()
            self.object.log_action(request.user, 
                "Processed CSV and created %d new data groups." % self.object.data_groups.count())
        return HttpResponseRedirect(self.object.get_absolute_url())

class DeploymentCreateView(generic.edit.FormView):
    template_name = "deployments/deployment_form.html"
    form_class = DeploymentCreateFromCSVForm

    def form_valid(self, form):
        csv_file = form.cleaned_data.get('csv_file', None)
        try:
            self.new_deployment = Deployment.create_from_csv_file(csv_file)
            self.new_deployment.log_action(self.request.user, "Created.")
        except Deployment.AlreadyExists, e:
            msg = "%s %s." % (e.message, e.html_link_to_object("View the deployment"))
            messages.warning(self.request, msg)
            return super(DeploymentCreateView, self).form_invalid(form)
        else:
            self.new_deployment.process_csv()
            self.new_deployment.log_action(self.request.user, 
                "Processed CSV and created %d new data groups." % self.new_deployment.data_groups.count())
        return super(DeploymentCreateView, self).form_valid(form)

    def get_success_url(self):
        return self.new_deployment.get_absolute_url()

class IngestionCreateView(generic.edit.CreateView):
    template_name = "deployments/ingestion_form.html"
    form_class = IngestionForm

    def form_valid(self, form):
        for field in INGESTOR_OPTIONS:
            if not getattr(form.instance, field):
                setattr(form.instance, field, INGESTOR_OPTIONS[field])
        form.instance.deployment = Deployment.get_by_designator(
            self.kwargs.pop('deployment_designator', None))
        return super(IngestionCreateView, self).form_valid(form)

class IngestionDetailView(generic.DetailView):
    model = Ingestion

    def get_object(self):
        try:
            object = Ingestion.objects.get(**{
                'deployment': Deployment.get_by_designator(self.kwargs.pop('deployment_designator', None)),
                'index': self.kwargs.pop('index', None),
                })
            return object
        except:
            raise Http404(u"No ingestion found.")

    def post(self, request, *args, **kwargs):
        self.object = self.get_object()
        if "_ingest" in request.POST:
            self.object.log_action(request.user, "Ingesting deployment.")
            tasks.ingest.delay(self.object, annotations={'user': request.user, })
        return HttpResponseRedirect(self.object.get_absolute_url())
