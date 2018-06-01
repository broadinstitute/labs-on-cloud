import os
import argparse
from subprocess import check_call
import loc


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Archive a file or directory in tar.xy format')
    parser.add_argument('--path',
                        help='File or directory to archive',
                        required=True)
    parser.add_argument('--dest',
                        help='gs:// URL to archive to',
                        required=True)
    parser.add_argument('--tmpdir',
                        help='Temporary directory to create archive if input path is a directory', default='/tmp')

    args = parser.parse_args()
    result = loc.do_archive(path=args.path, dest=args.dest, tmpdir=args.tmpdir)
