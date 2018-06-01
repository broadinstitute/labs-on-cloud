import os
import argparse
from subprocess import check_call


def do_archive(path, dest, tmpdir='/tmp'):
    tmp_file = None
    path = os.path.abspath(path)
    # original_path = path
    if os.path.isdir(path):
        tmp_file = os.path.join(tmpdir, os.path.basename(path)) + '.tar.xz'
        check_call(['tar', '-C', os.path.dirname(path), '-cJf', tmp_file, os.path.basename(path)])
        path = tmp_file
    elif not os.path.isfile(path):
        raise FileNotFoundError(path + ' not found.')
    file_size = os.path.getsize(path)
    check_call(['gsutil', '-m', 'cp', path, dest])
    if tmp_file is not None:
        os.remove(tmp_file)
    return {'path': path, 'archive_size': file_size}
