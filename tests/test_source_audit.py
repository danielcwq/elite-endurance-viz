import unittest

from enduranceviz.source_audit import classify_conflict, numeric_value, time_value


class SourceAuditTests(unittest.TestCase):
    def row(self, **changes):
        return {'Date Range': 'Activities for 1 Jan 2024 - 7 Jan 2024',
                'Distance (km)': '10.0', 'Time': '1h 0m', 'Elevation (m)': '100', **changes}

    def test_identical_and_exact_numeric_format(self):
        self.assertEqual(classify_conflict([self.row(), self.row()]), 'identical')
        self.assertEqual(classify_conflict([self.row(), self.row(**{
            'Distance (km)': '10', 'Time': '60m', 'Elevation (m)': '100.0'})]),
            'equivalent_numeric_or_time_format')

    def test_missing_is_not_zero(self):
        self.assertNotEqual(numeric_value(None), numeric_value(0))
        self.assertEqual(classify_conflict([self.row(), self.row(**{'Distance (km)': None})]),
                         'different_totals_or_missing_fields')

    def test_small_numeric_differences_are_not_silently_rounded_away(self):
        self.assertEqual(classify_conflict([self.row(), self.row(**{'Distance (km)': '10.00000001'})]),
                         'different_totals_or_missing_fields')

    def test_no_data_conflict_takes_precedence_over_totals(self):
        self.assertEqual(classify_conflict([self.row(), self.row(**{
            'Date Range': 'Week 01 - No Data', 'Distance (km)': 0})]), 'no_data_vs_summary')

    def test_different_ranges_are_not_equivalent(self):
        self.assertEqual(classify_conflict([self.row(), self.row(**{'Date Range': 'unrecognized'})]),
                         'different_date_range_text')

    def test_unrecognized_time_is_not_zero_and_duration_units_are_explicit(self):
        self.assertNotEqual(time_value(''), time_value('0h 0m'))
        self.assertNotEqual(time_value('unknown'), time_value('0h 0m'))
        self.assertEqual(time_value('1h 2m 3s'), ('seconds', 3723))


if __name__ == '__main__':
    unittest.main()
