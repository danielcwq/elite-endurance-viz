"""Safety checks for the local-only P1 evidence rebuild."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.build_p1_evidence_2024 import build, fingerprints


class P1EvidenceBuildTests(unittest.TestCase):
    def test_existing_output_is_refused_before_opening_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch('scripts.build_p1_evidence_2024.duckdb.connect') as connect:
                with self.assertRaises(FileExistsError):
                    build(Path('nonexistent.duckdb'), Path(temporary))
                connect.assert_not_called()

    def test_fingerprints_are_sorted_and_detect_content_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first, second = root / 'a', root / 'b'
            first.write_text('first')
            second.write_text('second')
            with patch('scripts.build_p1_evidence_2024.ROOT', root):
                before = fingerprints([second, first])
                self.assertEqual([row['path'] for row in before], ['a', 'b'])
                second.write_text('changed')
                after = fingerprints([first, second])
                self.assertEqual(before[0], after[0])
                self.assertNotEqual(before[1]['sha256'], after[1]['sha256'])


if __name__ == '__main__':
    unittest.main()
