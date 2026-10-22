import unittest
from unittest import mock
from pathlib import Path
import tempfile
from types import SimpleNamespace
import verify
from benchmark import payload, cells
from benchmark_support import quantiles


class BenchmarkTests(unittest.TestCase):
    def test_payload_size_and_identity(self):
        for size in [64, 1024, 16384]:
            value=payload(421,size)
            self.assertEqual(len(value.encode()),size)
            self.assertEqual(int(value[:10]),421)
            self.assertEqual(value,payload(421,size))

    def test_axis_coverage(self):
        self.assertEqual({c['consumers'] for c in cells()},{1,4,16})
        self.assertEqual({c['producers'] for c in cells()},{1,4,16})
        self.assertEqual(quantiles([2,1,3])['p99'],3)

    def test_posix_cluster_launch_owns_the_group_used_by_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster=verify.Cluster.__new__(verify.Cluster)
            cluster.path=Path(directory)
            cluster.processes=[]
            cluster.record=lambda *args,**kwargs:None
            with mock.patch.object(verify,'os',SimpleNamespace(name='posix')), mock.patch.object(verify.subprocess,'Popen') as popen:
                popen.return_value.pid=123
                cluster.start({'name':'node'})
                self.assertIs(popen.call_args.kwargs['start_new_session'],True)
