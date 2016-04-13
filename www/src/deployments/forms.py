from django import forms

from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Fieldset, ButtonHolder, Submit, Div

from deployments import models
from deployments.settings import INGESTOR_OPTIONS

HELP_TEXTS = {k: 'Leave blank to use default value: <em>%s</em>' % v for k, v in INGESTOR_OPTIONS.iteritems()}
DESCRIPTIVE_HELP_TEXTS = {
    'test_mode': 'Test Mode will run through the ingestion process without pushing any files to the QPID queues.', 
    'force_mode': 'Force Mode will ignore EDEX log files for records of previous ingestions and ingest all matching files.',
    'no_edex': 'The Ingestor will automatically check to see if EDEX services are alive between each file ingestion. Selecting this option will turn this off.',
    }

for k in HELP_TEXTS:
    HELP_TEXTS[k] = "%s %s" % (DESCRIPTIVE_HELP_TEXTS.get(k, ""), HELP_TEXTS[k])

class DeploymentCreateFromCSVForm(forms.Form):
    csv_file = forms.FileField(label="CSV File")

class IngestionForm(forms.ModelForm):
    class Meta:
        model = models.Ingestion
        exclude = ('deployment', 'timestamp', 'status', 'index')
        help_texts = HELP_TEXTS

    def __init__(self, *args, **kwargs):
        self.helper = FormHelper()
        self.helper.form_id = 'id-ingestionForm'
        self.helper.form_method = 'post'
        self.helper.form_action = ''
        self.helper.layout = Layout(
            Div(
                Div(
                    Fieldset('File Ingestion', 'sleep_timer', 'start_date', 'end_date', 'quick_look_quantity'),
                    Fieldset('Switches', 'test_mode', 'force_mode', 'no_edex', 'health_check_enabled', 'active'),
                    css_class="col-md-6", ),
                Div(
                    Fieldset('QPID', 'qpid_host', 'qpid_port', 'qpid_user', 'qpid_password'),
                    Fieldset('EDEX', 'cooldown', 'edex_command'),
                    css_class="col-md-6", ),
                Div(
                    ButtonHolder(Submit('submit', 'Submit'), css_class="col-md-6"),
                    css_class="col-md-12", ),
                )
            )
        super(IngestionForm, self).__init__(*args, **kwargs)
