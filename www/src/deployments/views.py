from github import Github
from StringIO import StringIO

from django.contrib import messages
from django.http import Http404, HttpResponseRedirect
from django.shortcuts import render
from django.views import generic 

from deployments.models import Deployment, Ingestion
from deployments.forms import DeploymentCreateFromCSVForm, IngestionForm
from deployments import tasks
from deployments.settings import INGESTOR_OPTIONS, GITHUB_TOKEN

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
        except Deployment.BadDesignator, e:
            msg = "%s Check that the file name is correct." % (e, message)
            messages.warning(self.request, e.message)
            return super(DeploymentCreateView, self).form_invalid(form)
        else:
            self.new_deployment.process_csv()
            self.new_deployment.log_action(self.request.user, 
                "Processed CSV and created %d new data groups." % self.new_deployment.data_groups.count())
        return super(DeploymentCreateView, self).form_valid(form)

    def get_success_url(self):
        return self.new_deployment.get_absolute_url()

class DeploymentGithubImportView(generic.TemplateView):
    template_name = "deployments/deployment_github_import.html"

    def post(self, request, *args, **kwargs):
        if "_import" in request.POST:
            repository = [
                o for o 
                in Github(GITHUB_TOKEN).get_user().get_orgs() 
                if o.login=='ooi-integration'
                ][0].get_repo('ingestion-csvs')

            def get_csvs(repo, filepath, _csv_files={}):
                for item in repo.get_dir_contents(filepath):
                    if "#" in item.path:
                        continue
                    if item.type == "dir":
                        print item.path
                        _csv_files = get_csvs(repo, item.path)
                    elif item.path.endswith(".csv"):
                        print item.path
                        _csv_files[item.path] = item.decoded_content
                return _csv_files

            csv_files = get_csvs(repository, ".")

            for f in csv_files:
                csv_file = csv_files[f]
                new_deployment, created = Deployment.create_or_update_from_content(f, csv_file)
                print new_deployment.designator, created
                new_deployment.process_csv()
                print "processed"
                if created:
                    new_deployment.log_action(self.request.user, "Created.")
                else:
                    new_deployment.log_action(self.request.user, "Updated from Github.")
                new_deployment.log_action(self.request.user, 
                    "Processed CSV and created %d new data groups." % new_deployment.data_groups.count())
        return super(DeploymentGithubImportView, self).dispatch(request, *args, **kwargs)


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
