from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

import duckdb
from starlette.responses import PlainTextResponse

import main
from enduranceviz.geography import COUNTRY_CENTROIDS, NON_GEOGRAPHIC_CODES


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

    def test_map_covers_every_geographic_nationality_code(self) -> None:
        database = ROOT / "deploy/enduranceviz_2024.duckdb"
        with duckdb.connect(str(database), read_only=True) as connection:
            codes = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT nationality_code FROM athletes WHERE nationality_code IS NOT NULL"
                ).fetchall()
            }
        self.assertEqual(codes - set(COUNTRY_CENTROIDS), NON_GEOGRAPHIC_CODES)

    def test_homepage_exposes_lazy_nationality_map(self) -> None:
        rendered = str(main.homepage())
        self.assertIn("Explore map", rendered)
        self.assertIn("country-map", rendered)
        self.assertIn("/api/map/countries", rendered)


if __name__ == "__main__":
    unittest.main()
