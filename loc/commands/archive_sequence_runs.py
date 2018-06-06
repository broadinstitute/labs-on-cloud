import json
from jira import JIRA
import logging.config
import loc
import datetime
import argparse
from subprocess import check_call

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Archive sequencing runs in tar.xy format')
    parser.add_argument('--dir',
                        help='Sequencing directory',
                        required=True, action='append')
    parser.add_argument('--dest',
                        help='gs:// base URL to archive to',
                        required=True)
    parser.add_argument('--project',
                        help='JIRA project id or key',
                        required=True)
    parser.add_argument('--days_old',
                        help='Number of days old a run must be in order to archive',
                        required=True, type=int)
    parser.add_argument('--config',
                        help='Application config (logging, jira)')
    parser.add_argument('--delete_path',
                        help='Delete the sequencing run after completion.',
                        action='store_true')
    parser.add_argument('--dry_run',
                        help='Only print list of run ids eligible for archiving',
                        action='store_true')
    parser.add_argument('--tmpdir',
                        help='Temporary directory to create archive', default='/tmp')

    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)
    check_call(['gsutil'])  # ensure gsutil is in path
    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger()
    jira = JIRA(**config['jira'])

    project_id = args.project
    days_old = args.days_old
    dest_base_url = args.dest
    if dest_base_url[len(dest_base_url) - 1] != '/':
        dest_base_url += '/'

    field_map = loc.get_field_name_to_id(jira)
    for run in loc.filter_flow_cells_by_run_date(sequencing_dirs=args.dir, days_old=days_old,
                                                 now=datetime.datetime.now()):
        issues = jira.search_issues(
            'project=' + project_id + ' AND issuetype=sequencing_run AND "Sequencing Run ID" ~ "' +
            run['run_id'] + '" AND (status=SEQUENCED OR status="Archive Failure")')
        if len(issues) == 1:
            issue = issues[0]
            if args.dry_run:
                print(str(run['run_id']))
            else:
                jira.transition_issue(issue, 'To Archiving')
                try:
                    archive_url = dest_base_url + run['run_id']
                    archive_result = loc.do_archive(run['path'], archive_url, tmpdir=args.tmpdir)
                    now = datetime.datetime.now()
                    jira.transition_issue(issue, 'To Archived',
                                          {field_map['archive_size']: archive_result['archive_size'],
                                           field_map['archive_url']: archive_url,
                                           field_map['archive_date']: str(now.year) + '-' + str(
                                               now.month) + '-' + str(now.day)})

                    if args.delete_path:
                        check_call(['rm', '-rf', run['path']])
                except Exception as ex:
                    jira.transition_issue(issue, 'Archive Failure')
                    logger.error(str(ex))
        elif len(issues) > 1:
            logger.error('More than one run id found for ' + run['run_id'])
