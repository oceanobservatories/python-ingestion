from deployments.settings import INGESTOR_OPTIONS

def ingestor_options(request):
    return {'ingestor_defaults': INGESTOR_OPTIONS}