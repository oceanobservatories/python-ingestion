from celery import shared_task
from datetime import datetime

from triage.models import LogEvent

def get_log_header_parts(line):
    parts = line.strip().split(' ', 4)
    i = 0
    while i < len(parts):
        if parts[i] == '':
            parts.pop(i)
        else:
            i += 1
    return parts

def get_start_data(line):
    try:
        p = line.split('* Processing file: ')
        return {'type': 'ingest_start', 'filename': p[1].strip()}
    except:
        return None

def get_end_data(line):
    try:
        p = line.split('* Finished Processing file: ')
        return {'type': 'ingest_end', 'filename': p[1].strip()}
    except:
        return None

def get_prov_element(line, key):
    v = line.split('%s=' % key)[1]
    i = v.find(',')
    if i == -1:
        i = v.index(']')
    return v[:i].replace(']', '')

def get_prov_data(line):
    try:
        d = {'type': 'ingest_prov'}
        d['subsite'] = get_prov_element(line, 'subsite')
        d['node'] = get_prov_element(line, 'node')
        d['sensor'] = get_prov_element(line, 'sensor')
        d['method'] = get_prov_element(line, 'method')
        d['uuid'] = get_prov_element(line, 'uuid')
        d['deployment'] = int(get_prov_element(line, 'deployment'))
        d['filename'] = get_prov_element(line, 'fileName')
        d['parser_name'] = get_prov_element(line, 'parserName')
        d['parser_version'] = get_prov_element(line, 'parserVersion')
        return d
    except:
        return None

def get_part_count_data(line):
    try:
        d = {'type': 'ingest_part_count'}
        d['filename'] = line.split('File: ')[1].split(' ', 1)[0]
        d['particle_count'] = int(line.split('Status: ')[1].split(' ', 2)[1])
        return d
    except:
        return None

@shared_task
def save_log(line):
    header_parts = get_log_header_parts(line)
    try:
        level = header_parts[0]
        timestamp = datetime.strptime('%s %s' % (header_parts[1], header_parts[2]),
                                      '%Y-%m-%d %H:%M:%S,%f')
        p = header_parts[3].split('] ', 1)
        route = p[0].replace('[Ingest.', '')
        data = {'level': level, 'timestamp': timestamp, 'route': route}
        if level in ['ERROR', 'WARN']:
            data['type'] = 'ingest_error'
            data['filename'] = ''
            data['error_details'] = p[1]
        elif '* Processing file:' in p[1]:
            data.update(get_start_data(p[1]))
        elif '* Finished Processing file:' in p[1]:
            data.update(get_end_data(p[1]))
        elif 'Adding provenance' in p[1]:
            data.update(get_prov_data(p[1]))
        elif 'ParticleFactory' in p[1]:
            data.update(get_part_count_data(p[1]))
        else:
            return
        print 'Adding log: %s' % data
        # LogEvent.objects.create(**data)
    except:
        pass
