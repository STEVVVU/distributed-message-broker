import unittest
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
