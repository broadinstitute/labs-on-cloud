#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from subprocess import check_call, CalledProcessError
import loc


class TestListFlowCells(unittest.TestCase):

    def test_list_flow_cells(self):
        from datetime import datetime
        import os
        results = loc.filter_flow_cells_by_run_date(sequencing_dirs=['seq_dir'], days_old=6,
                                                    now=datetime(year=2018, month=5, day=21))
        self.assertEqual('170622_M01581_1108_000000000-B949Y', os.path.basename(results[0]['path']))
        results = loc.filter_flow_cells_by_run_date(sequencing_dirs=['seq_dir'], days_old=6,
                                                    now=datetime(year=2019, month=5, day=21))
        self.assertEqual(len(results), 3)
