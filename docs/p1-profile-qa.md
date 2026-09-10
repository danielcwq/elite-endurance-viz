# P1 profile edge cases and share metadata

Implemented and checked on the P1 branch; not a production deployment or a claim
that the entire product QA checklist is complete.

## Coverage of this pass

[Automated HTTP/HTML checks](../tests/test_profile_qa.py) exercise the current
FastHTML app against the packaged database opened read-only. The largest activity
history in each existing legacy coverage class is selected deterministically.
The artifact currently contains `high`, `moderate`, `low`, and `unknown` classes,
but no `insufficient` rows. [Synthetic profiles](synthetic-demo.md) exercise that
missing class without changing the real snapshot or inventing an observed example.
These labels select QA cases; they do not filter the P1 study.

| Check | Evidence |
| --- | --- |
| Coverage-class profiles | All four existing classes return 200, one H1, one document title, and 53 accessible weekly rows. Synthetic insufficient profiles render too. |
| No recorded running | No running charts; summary values say `Unavailable`, not zero training. Metadata reports the recorded-week count and incomplete-history caveat. |
| Missing performance data | The real registry athlete without performance rows renders explicit missing-performance and missing-primary-event messages. |
| Unusual activity types | Actual WeightTraining, VirtualRide, and TrailRun records remain visible under date/category filters. |
| Invalid links | Malformed/nonexistent UUIDs return 404; invalid page/date/category parameters return 400. |
| Chart labels | Every rendered SVG's accessible title/description reference resolves to a nonempty element; page IDs are unique. |
| Source warnings | Retained warning weeks are counted in the profile's visible warning and share description. Absence of a warning is not called complete capture. |
| Metadata safety | Titles and attribute values escape markup; adversarial text does not create a script element. |

## Share metadata

[The metadata helper](../enduranceviz/page_metadata.py) supplies one document title,
a standard description, matching Open Graph title/description/type/site name,
and Twitter summary-card title/description. Real profile descriptions report the
2024 snapshot, primary event when available, and number of full UTC weeks with
recorded runs. Source-warning running-week counts are included when nonzero.

Activity-table filters do not change full-year summary definitions. Accordingly,
profile metadata retains full-year recorded-week coverage even on filtered URLs.
No annual training estimate, statistical confidence, eligibility classification,
or performance comparison is introduced by this change.

Synthetic home/profile titles identify the demo, descriptions say the data is
invented, and `robots=noindex, nofollow` is emitted. That is a crawler instruction,
not access control; the separate builder/packager safeguards still apply.

No canonical/`og:url` pointing to production is forced onto a branch or local
preview, and no social image is invented. The generated HTML is verified;
third-party social-crawler rendering, caching, image cards, and truncation have
not been tested. Descriptions may be shortened by those services.

## Still open

- Keyboard traversal and horizontal scrolling need browser QA; resolving chart
  accessible labels is not a keyboard or screen-reader audit.
- Mobile layout, screenshot-visible caveats, and external share previews remain
  separate checks. These tests do not prove those visual behaviours.
- Empty/undersized **cohorts** await the cohort interface and approved inclusion
  decisions; empty athlete profiles are not a substitute for those cases.
- New charts and later study/editorial pages need their own QA when implemented.

Reproduce this pass with:

```sh
.venv/bin/python -m unittest discover -s tests -p test_profile_qa.py
make check
```
