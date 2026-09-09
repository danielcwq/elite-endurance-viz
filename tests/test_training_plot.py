import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec('matplotlib'), 'Optional requirements-analysis.txt needed for figure tests')
class TrainingPlotTests(unittest.TestCase):
    def test_shared_axes_include_both_groups_and_all_points(self):
        from scripts.plot_event_training_2024 import plot
        rows = [
            {'event':'800m', 'recorded_sex':'female', 'distance_km':10, 'frequency':2,
             'recorded_run_weeks':4, 'distance_measured_weeks':4},
            {'event':'800m', 'recorded_sex':'male', 'distance_km':200, 'frequency':30,
             'recorded_run_weeks':5, 'distance_measured_weeks':5},
        ]
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'figure.json'
            source.write_text(json.dumps({'events':['800m'], 'athlete_summaries':rows}))
            observed = []
            def capture(figure, *args, **kwargs):
                observed.extend(ax.get_xlim() for ax in figure.axes)
            with patch('matplotlib.figure.Figure.savefig', autospec=True, side_effect=capture):
                plot(source, Path(temporary) / 'figure.png')
        self.assertEqual(len(observed),4)
        for index, limit in enumerate(observed):
            self.assertEqual(limit[0],0)
            self.assertGreater(limit[1],200 if index % 2 == 0 else 30)


if __name__ == '__main__':
    unittest.main()
