#!/usr/bin/env python3
"""Plot approved exploratory athlete summaries with local scientific Python."""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def plot(source, output):
    payload = json.loads(source.read_text())
    rows, events = payload['athlete_summaries'], payload['events']
    fig, axes = plt.subplots(2, 2, figsize=(15, 9), sharex='col')
    fig.subplots_adjust(left=.17, right=.97, top=.83, bottom=.16, hspace=.40, wspace=.45)
    labels = {'3000m Steeplechase': '3000m SC', 'Half Marathon': 'Half marathon'}
    for row_index, (sex, title, color) in enumerate((('female', 'Women', '#166b86'), ('male', 'Men', '#8050a0'))):
        for col_index, (field, axis_title) in enumerate((('distance_km', 'Recorded-week running distance (km)'), ('frequency', 'Recorded-week run count'))):
            ax = axes[row_index, col_index]
            group_labels = []
            for y, event in enumerate(events):
                group = [r for r in rows if r['event'] == event and r['recorded_sex'] == sex and r[field] is not None]
                values = np.array([r[field] for r in group])
                week_field = 'distance_measured_weeks' if field == 'distance_km' else 'recorded_run_weeks'
                weeks = np.median([r[week_field] for r in group]) if group else None
                group_labels.append(f"{labels.get(event,event)}  ·  n={len(group)}" + (f"  ·  w={weeks:g}" if weeks is not None else ''))
                if not len(values):
                    continue
                # Deterministic vertical jitter is display-only, not a data transformation.
                jitter = np.random.default_rng(100 + y + row_index*10 + col_index*100).uniform(-.15,.15,len(values))
                ax.scatter(values, y+jitter, s=15, color=color, alpha=.40, linewidths=0, zorder=2)
                q1, med, q3 = np.quantile(values, [.25,.5,.75])
                ax.plot([q1,q3], [y,y], color='#192c38', linewidth=3, zorder=3)
                ax.scatter([med], [y], marker='D', color='#d55b22', s=35, edgecolor='white', linewidth=.7, zorder=4)
            ax.set_yticks(range(len(events)), group_labels, fontsize=9)
            ax.set_ylim(len(events)-.5,-.5)
            # Shared axes must include both sex groups; fixing the first panel's
            # limits early would silently clip larger values in the second row.
            maximum = max((r[field] for r in rows if r[field] is not None), default=1)
            ax.set_xlim(0, max(1, maximum) * 1.05)
            ax.tick_params(axis='x', labelbottom=True)
            ax.set_title(f'{title} · {axis_title}', loc='left', fontsize=12, fontweight='bold', pad=14)
            ax.grid(axis='x', color='#dce3e7', linewidth=.6)
            ax.set_axisbelow(True)
            ax.spines[['top','right','left']].set_visible(False)
            ax.spines['bottom'].set_color('#bcc7cd')
            ax.tick_params(axis='y', length=0, pad=8)
    fig.suptitle('Observed running weeks across event specializations', x=.04, y=.965, ha='left', fontsize=21, fontweight='bold')
    fig.text(.04,.913,'2024 snapshot · Each dot is one athlete’s median over weeks with recorded runs.', fontsize=12)
    fig.text(.04,.881,'Orange diamond: group median   |   Dark line: middle 50% of athlete medians (not a confidence interval)', fontsize=10, color='#42515b')
    fig.text(.04,.095,'n = contributing athletes; w = median contributing weeks. No annual-week cutoff or P0 eligibility filter.', fontsize=10)
    fig.text(.04,.068,'These are recorded-running-week summaries, not complete training estimates. Distance excludes weeks with missing/invalid run distances.', fontsize=10)
    fig.text(.04,.041,'Sparse posting and source warnings remain. Half-marathon women: only 3 contributors. Exploratory; no population inference.', fontsize=10)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=150, facecolor='white')
    plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    plot(args.source, args.output)
