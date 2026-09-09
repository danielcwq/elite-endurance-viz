from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

from starlette.responses import PlainTextResponse

import main


ROOT = Path(__file__).resolve().parents[1]


class DeploymentContractTests(unittest.TestCase):
    def test_unknown_athlete_is_a_real_not_found_response(self) -> None:
        response = main.get_athlete("not-a-uuid")
        self.assertIsInstance(response, PlainTextResponse)
        self.assertEqual(response.status_code, 404)

    def test_data_tree_remains_excluded_from_vercel(self) -> None:
        active_rules = [
            line.strip()
            for line in (ROOT / ".vercelignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertIn("data/", active_rules)

    def test_packaged_database_matches_checksum_manifest(self) -> None:
        manifest = json.loads((ROOT / "deploy/serving-artifact.json").read_text(encoding="utf-8"))
        artifact = ROOT / "deploy" / manifest["artifact"]
        self.assertTrue(artifact.is_file())
        self.assertEqual(artifact.stat().st_size, manifest["bytes"])

        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        self.assertEqual(digest, manifest["sha256"])


if __name__ == "__main__":
    unittest.main()
