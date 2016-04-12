from django.contrib import messages
from django.http import Http404, HttpResponseRedirect
from django.shortcuts import render
from django.views import generic 

from deployments.models import Deployment
from deployments.forms import DeploymentCreateFromCSVForm
from deployments import tasks

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
        if "_ingest" in request.POST:
            self.object.log_action(request.user, "Ingesting deployment.")
            tasks.ingest.delay(self.object, annotations={'user': request.user, })
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
