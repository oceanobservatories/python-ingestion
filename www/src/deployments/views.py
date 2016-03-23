from django.http import Http404
from django.shortcuts import render
from django.views import generic 

from deployments.models import Deployment
from deployments.forms import DeploymentCreateFromCSVForm

class DeploymentListView(generic.ListView):
    model = Deployment

class DeploymentDetailView(generic.DetailView):
    model = Deployment

    def get_object(self):
        try:
            return Deployment.get_by_designator(self.kwargs.pop('slug', None))
        except:
            raise Http404(u"No deployment found.")

class DeploymentCreateView(generic.edit.FormView):
    template_name = "deployments/deployment_form.html"
    form_class = DeploymentCreateFromCSVForm
    success_url = "/deployments/"

    def form_valid(self, form):
        csv_file = form.cleaned_data.get('csv_file', None)
        new_deployment = Deployment.create_from_csv_file(csv_file)
        new_deployment.process_csv()
        return super(DeploymentCreateView, self).form_valid(form)