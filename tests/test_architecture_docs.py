"""Keep the documented physical schema aligned with executable constraints."""

import re
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DOCUMENT = ROOT / 'docs/architecture.md'


class ArchitectureDocumentationTests(unittest.TestCase):
    def test_er_diagram_matches_all_tables_foreign_keys_and_shown_columns(self):
        text = DOCUMENT.read_text(encoding='utf-8')
        diagram = re.search(r'```mermaid\nerDiagram\n(.*?)```', text, re.S).group(1)
        entities = dict(re.findall(r'^    (\w+) \{\n(.*?)^    \}', diagram, re.M | re.S))
        relationships = set(re.findall(r'^    (\w+) \|\|--o[{|] (\w+) :', diagram, re.M))
        with duckdb.connect(':memory:') as c:
            c.execute((ROOT / 'schema/2024.sql').read_text(encoding='utf-8'))
            tables = {row[0] for row in c.execute('SHOW TABLES').fetchall()}
            keys = {(parent, child) for child, parent in c.execute('''
                SELECT table_name, referenced_table FROM duckdb_constraints()
                WHERE constraint_type='FOREIGN KEY'
            ''').fetchall()}
            columns = {(table, name): kind for table, name, kind in c.execute('''
                SELECT table_name,column_name,data_type FROM information_schema.columns
                WHERE table_schema='main'
            ''').fetchall()}
        self.assertEqual(set(entities), tables)
        self.assertEqual(relationships, keys)
        self.assertEqual(len(relationships), 10)
        for table, body in entities.items():
            for line in body.splitlines():
                kind, name, *_ = line.split()
                # Mermaid abbreviates the SQL timezone type for readability.
                expected_type = 'TIMESTAMP WITH TIME ZONE' if kind == 'TIMESTAMPTZ' else kind
                self.assertEqual(columns[table, name], expected_type)

    def test_relative_document_links_resolve(self):
        text = DOCUMENT.read_text(encoding='utf-8')
        targets = re.findall(r'\]\(([^)]+)\)', text)
        self.assertGreater(len(targets), 10)
        for target in targets:
            if '://' not in target:
                self.assertTrue((DOCUMENT.parent / target.split('#')[0]).is_file(), target)


if __name__ == '__main__':
    unittest.main()
