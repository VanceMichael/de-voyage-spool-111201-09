import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        self.assertEqual(member_path('data/a.csv'), 'data/a.csv')
        self.assertEqual(collect(self.payload)[0]['size'], 4)
        with self.assertRaises(ValueError):
            member_path('../bad')

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
