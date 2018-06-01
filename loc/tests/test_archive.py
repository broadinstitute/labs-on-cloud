#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from subprocess import check_call, CalledProcessError
import loc
import os


class TestArchive(unittest.TestCase):

    def test_non_existent_dest(self):
        self.assertRaises(CalledProcessError, loc.do_archive, path='.', dest='gs://i-dont-exist/')

    def test_non_existent_src(self):
        self.assertRaises(FileNotFoundError, loc.do_archive, path='/i/dont/exist', dest='gs://i-dont-exist/')

    def test_archive_and_unarchive_diff(self):
        dest = 'gs://my_test_bucket/test/foo.tar.xy'
        loc.do_archive(path='seq_dir', dest=dest)
        os.mkdir('unarchive_test')
        check_call(['gsutil', '-m', 'cp', dest, 'unarchive_test/foo.tar.xy'])
        check_call(['tar', 'xf', 'unarchive_test/foo.tar.xy', '-C', 'unarchive_test'])
        check_call(['diff', 'seq_dir', 'unarchive_test/seq_dir'])
