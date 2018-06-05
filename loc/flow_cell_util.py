import xml.etree.ElementTree as ET
import os
from datetime import datetime, timedelta
import loc
import dateutil.parser


def create_flowcells(sequencing_dirs, jira, project_id, verbose=False):
    field_map = loc.get_field_name_to_id(jira)
    file_names_to_attach = ['RunInfo.xml'.lower(), 'RunParameters.xml'.lower()]
    for run in loc.list_flow_cells(sequencing_dirs):
        issues = jira.search_issues(
            'project=' + project_id + ' AND issuetype=sequencing_run AND "Sequencing Run ID" ~ ' + run[
                'run_id'])
        if len(issues) == 0:  # no duplicates
            flow_cell_path = run['path']
            attachments = []
            files = os.listdir(flow_cell_path)
            for f in files:
                if f.lower() in file_names_to_attach:
                    attachments.append(os.path.join(flow_cell_path, f))
            if len(attachments) != len(file_names_to_attach):
                raise ValueError('Attachments not found')
            if verbose:
                print(run['run_id'])
            issue = jira.create_issue(fields={
                'project': project_id,
                field_map['flowcell']: run['flowcell'],
                'summary': run['flowcell'],
                field_map['instrument']: run['instrument'],
                field_map['run_date']: str(run['run_date'].year) + '-' + str(run['run_date'].month) + '-' + str(
                    run['run_date'].day),
                field_map['Sequencing Run ID']: run['run_id'],
                'issuetype': {'name': 'sequencing_run'}
            })

            for attachment in attachments:
                jira.add_attachment(issue=issue, attachment=attachment)
            jira.transition_issue(issue, 'To Sequenced')
        elif len(issues) > 1:
            raise ValueError('Duplicate run id:' + run['run_id'])


def filter_flow_cells_by_run_date(sequencing_dirs, days_old=180, now=datetime.now()):
    """
    :param sequencing_dirs (list): list of paths
    :param days_old (int): minimum age of sequencing run to pass filter
    :return:list of paths
    """
    results = []
    date_months_ago = now - timedelta(days=days_old)
    for run in loc.list_flow_cells(sequencing_dirs):
        if run['run_date'] <= date_months_ago:
            results.append(run['path'])
    return results


def list_flow_cells(sequencing_dirs):
    year_start = str(datetime.now().year)[0:2]
    for sequencing_dir in sequencing_dirs:
        flow_cells = os.listdir(sequencing_dir)
        for flow_cell_dir in flow_cells:
            flow_cell_path = os.path.abspath(os.path.join(sequencing_dir, flow_cell_dir))
            if os.path.isdir(flow_cell_path) and os.path.isfile(
                    os.path.join(flow_cell_path, 'RTAComplete.txt')) and os.path.isfile(
                os.path.join(flow_cell_path, 'RunInfo.xml')):
                tree = ET.parse(os.path.join(flow_cell_path, 'RunInfo.xml'))
                run = tree.find('Run')
                run_id = run.attrib['Id']
                run_date = run.find('Date').text  # e.g. 180521 year, month, day or 3/19/2018 2:28:33 PM
                if len(run_date) == 6:
                    year = year_start + run_date[0:2]
                    month = run_date[2:4]
                    day = run_date[4:6]
                    run_date = datetime(year=int(year), month=int(month), day=int(day))
                else:
                    run_date = dateutil.parser.parse(run_date)
                if run_date > datetime.now():
                    raise ValueError(run_id + ' date is in the future.')
                instrument = run.find('Instrument').text
                flowcell = run.find('Flowcell').text
                d = {'flowcell': flowcell, 'run_date': run_date, 'instrument': instrument, 'run_id': run_id,
                     'path': flow_cell_path}
                yield d
