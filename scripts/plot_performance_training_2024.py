#!/usr/bin/env python3
"""Unfitted, event/sex-specific scatterplots; colour is metric-contributing weeks."""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
import numpy as np


def plot(source, output_prefix):
    payload = json.loads(Path(source).read_text())
    rows, events = payload['athlete_summaries'], payload['events']
    # The approved lead comparison appears first, followed by the context/extensions.
    events = [e for e in ('800m', '5000m') if e in events] + [e for e in events if e not in ('800m', '5000m')]
    if not events:
        raise ValueError('No study events supplied')
    outputs = []
    for metric, week_field, label in (
        ('distance_km', 'distance_measured_weeks', 'Median recorded-week run distance (km)'),
        ('run_records', 'recorded_run_weeks', 'Median recorded-week Run-record count'),
    ):
        fig, axes = plt.subplots(len(events), 2, figsize=(13, 3*len(events)+3), squeeze=False)
        fig.subplots_adjust(left=.10, right=.86, top=.89, bottom=.09, hspace=.64, wspace=.30)
        pairs = [r for r in rows if r['event'] in events and r['recorded_sex'] in ('female', 'male')
                 and r['points'] is not None and r[metric] is not None]
        scores = [r['points'] for r in pairs]
        low, high = (min(scores), max(scores)) if scores else (1100, 1101)
        pad = max(5, (high-low)*.04)
        maximum = max((r[metric] for r in pairs), default=1)
        norm = Normalize(vmin=1, vmax=52)
        for row_index, event in enumerate(events):
            for col_index, (sex, title) in enumerate((('female', 'Women'), ('male', 'Men'))):
                ax = axes[row_index, col_index]
                group = [r for r in pairs if r['event'] == event and r['recorded_sex'] == sex]
                if group:
                    ax.scatter([r['points'] for r in group], [r[metric] for r in group],
                               c=[r[week_field] for r in group], cmap='viridis', norm=norm,
                               s=27, edgecolors='#243340', linewidths=.35, alpha=.85)
                    weeks = np.median([r[week_field] for r in group])
                    detail = f'n={len(group)} · median contributing weeks={weeks:g}'
                else:
                    ax.text(.5,.5,'No paired observations', transform=ax.transAxes, ha='center')
                    detail = 'n=0'
                short_event = event.replace('3000m Steeplechase', '3000m SC')
                ax.set_title(f'{short_event} · {title}\n{detail}', loc='left', fontsize=10, pad=8)
                ax.set_xlim(low-pad, high+pad)
                ax.set_ylim(0, max(1, maximum)*1.07)
                ax.set_xlabel('Highest stored 2024 primary-event result points', fontsize=8)
                ax.set_ylabel('km / recorded week' if metric == 'distance_km' else 'Run records / recorded week', fontsize=9)
                ax.grid(color='#dce3e7', linewidth=.6)
                ax.set_axisbelow(True)
                ax.spines[['top','right']].set_visible(False)
                ax.tick_params(labelsize=8)
        fig.suptitle('Performance points and publicly recorded running', x=.055,y=.978,
                     ha='left', fontsize=19, fontweight='bold')
        fig.text(.055,.953,label, fontsize=13)
        fig.text(.055,.933,'2024 snapshot · One point per athlete, within their primary event and recorded-sex group.', fontsize=10)
        fig.text(.055,.917,'800m / 5000m lead; other events provide context. Shared axes; no fitted line or pooled statistic.', fontsize=10)
        color_ax = fig.add_axes([.90,.37,.014,.28])
        bar = fig.colorbar(ScalarMappable(norm=norm,cmap='viridis'), cax=color_ax, ticks=[1,13,26,39,52])
        bar.set_label('Weeks contributing to the plotted median (not confidence)', fontsize=9, labelpad=10)
        fig.text(.055,.055,'Full UTC weeks with recorded Runs only; no annual-week or P0 eligibility cutoff. Source-warning records remain.', fontsize=9)
        fig.text(.055,.042,'Distance requires all stored Run distances in that week. Run records are not independent training sessions.', fontsize=9)
        fig.text(.055,.029,'Pre-existing 1,100-point source floor; selective posting and small cells limit interpretation. Not complete training or causal evidence.', fontsize=9)
        output = Path(f'{output_prefix}-{metric}.png')
        output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output, dpi=150, facecolor='white')
        plt.close(fig)
        outputs.append(output)
    return outputs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output_prefix', type=Path)
    args = parser.parse_args()
    for output in plot(args.source, args.output_prefix):
        print(f'Wrote {output}')
