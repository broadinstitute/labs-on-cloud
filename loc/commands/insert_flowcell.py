import json
import logging.config
from jira import JIRA
import logging
import loc
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Archive sequencing runs in tar.xy format')
    parser.add_argument('--dir',
                        help='Sequencing directory',
                        required=True, action='append')
    parser.add_argument('--project',
                        help='JIRA project id',
                        required=True)
    parser.add_argument('--config',
                        help='Application config (logging, jira)')
    parser.add_argument('--verbose',
                        help='Print progress information',
                        action='store_true')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger()
    jira = JIRA(**config['jira'])
    loc.create_flowcells(sequencing_dirs=args.dir, jira=jira, project_id=args.project, verbose=args.verbose)
