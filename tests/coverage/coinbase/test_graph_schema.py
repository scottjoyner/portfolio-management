import unittest

from coinbase.src.graph.schema import schema_statements, GRAPH_CONSTRAINTS, GRAPH_INDEXES


class TestSchema(unittest.TestCase):
    def test_schema_statements(self):
        stmts = schema_statements()
        self.assertEqual(stmts, [*GRAPH_CONSTRAINTS, *GRAPH_INDEXES])
        self.assertTrue(all(isinstance(s, str) for s in stmts))
        self.assertIn("CREATE CONSTRAINT cg_asset_id", stmts[0])

    def test_counts(self):
        self.assertEqual(len(GRAPH_CONSTRAINTS), 8)
        self.assertEqual(len(GRAPH_INDEXES), 4)


if __name__ == "__main__":
    unittest.main()
