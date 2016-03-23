from django import forms

class DeploymentCreateFromCSVForm(forms.Form):
    csv_file = forms.FileField(label="CSV File")