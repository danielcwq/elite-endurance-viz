# P1 profile responsive and keyboard pass

Checked locally in the Codex in-app browser on the P1 branch. No production
deployment, statistical change, or data rewrite is part of this pass.

## Findings and changes

- The profile already kept horizontal overflow inside the charts/tables. Its
  evidence table was automatically keyboard-focusable in the tested browser, but
  had no explicit tab stop or accessible region label. It now has `tabindex=0`, a
  descriptive region name, and an explicit visible focus outline.
- All three charts and both tables have named keyboard-scrollable regions. The
  activity table now has an explicit focus outline too.
- At 320 px, the original pagination wrapped the second link to another row.
  Below a 600 px component width, previous/next links now occupy two stable columns;
  the page-number indicator is hidden, while the visible record-range summary
  remains. Wide layouts retain the page indicator.
- The original mobile activity cells were 14 px, descriptions about 12.25 px, and
  the activity column could leave only about 59 px for text. Cells and descriptions
  now use 16 px at phone widths, with a sensible minimum activity-column width and
  horizontal scrolling for the whole table. All data columns remain available.
- Mobile Apply/Reset and pagination controls have 48 px target heights. Disclosure
  summaries have a 48 px minimum height and readable wrapped line spacing. Wider
  layouts retain compact form controls and the existing coarse-pointer target aid.
- Navigation remains the existing search/back link. There is no desktop navigation
  menu to collapse or replace with a hamburger menu.

The make-responsive skill guided mobile-first text sizing, component breakpoints,
table containment, target sizes, and multi-width checks. No new navigation system
or analytics design was introduced.

## Measured rendered layout

The real Jack Balick profile was checked on both unfiltered and Run-filtered
second activity pages; this supplies previous/next links and long activity text.
Measurements below come from the rendered browser DOM, not CSS-source assertions.

| Viewport width | Page scroll width | Activity cell / description text | Pagination |
| --- | --- | --- | --- |
| 320 px | 320 px | 16 / 16 px | Two-column links |
| 390 px | 390 px | 16 / 16 px | Two-column links |
| 768 px | 768 px | 18 / 18 px | Links and page indicator |
| 1280 px | 1280 px | 17.5 / 17.5 px | Links and page indicator |

Phone charts retain their intentional 900 px plotting canvas inside the scroll
region; narrow activity tables similarly overflow only within their wrapper.
The page itself does not gain horizontal overflow in these cases. Screenshots
were inspected at narrow, tablet, and desktop sizes. Temporary viewport overrides
were reset after testing.

## Keyboard verification

At 390 px:

1. Enter opens the weekly-evidence disclosure.
2. Tab reaches the named weekly table. Right Arrow moves its horizontal offset
   from 0 to 40 px, with a visible 2 px focus outline.
3. Tab leaves that region for the methodology disclosure: no focus trap.
4. Each of the three named charts and the activity table accepts Right Arrow,
   advances its scroll offset, and displays the same visible outline.
5. The native category selector can be operated by keyboard. After selecting Run,
   Tab reaches Apply and Enter submits a GET URL with `category=Run`, resetting
   pagination to the first page. The rendered count was 1–30 of 309 records.
6. Enter on Next preserves the date/category parameters and adds `page=2`; the
   rendered count becomes 31–60 of 309 records.

[Regression tests](../tests/test_training_profile.py) require all five scroll
regions to have unique nonempty names, `role=region`, and `tabindex=0`, including
the evidence table in a no-run profile. Existing HTTP/filter tests continue to
check date semantics, pagination, and null/zero distinctions. These markup tests
complement, rather than replace, the browser checks above.

## Limits

This is profile QA in one desktop browser with viewport resizing, not physical
phone testing, a screen-reader audit, cross-browser certification, or a check of
the entire homepage/map and future cohort interface. Those remain open. No actual
social-network preview or viewport-independent screenshot claim is made.
