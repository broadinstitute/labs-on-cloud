import json
import sys
from jira import JIRA
import logging
import loc
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Archive sequencing runs in tar.xy format')
    parser.add_argument('--dir',
                        help='Sequencing directory',
                        required=True, action='append', nargs='+')
    parser.add_argument('--project',
                        help='JIRA project id or key',
                        required=True)
    parser.add_argument('--config',
                        help='Application config (logging, jira)')

    args = parser.parse_args()

    with open(sys.argv[1], 'r') as f:
        config = json.load(f)

    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger()
    jira = JIRA(config['jira'])
    project_id = args.project
    sequencing_dirs = args.dir
    loc.create_flowcells(sequencing_dirs=sequencing_dirs, jira=jira)
