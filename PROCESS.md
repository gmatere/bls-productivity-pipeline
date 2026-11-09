# Process

## Architecture

### Sourcing

BLS returns `403` to anonymous clients. Their access policy
(<https://www.bls.gov/bls/pss.htm>) says they may block robots that don't say
who they are, so the fix is the one the policy asks for: a `User-Agent` with a
contact address. That's following the rule, not getting around it.

Requests retry with backoff — five attempts, 1.5s up to 24s, on `429` and
`5xx`, honouring `Retry-After`. BLS almost certainly won't rate-limit thirteen
requests, but it's polite, and it means a brief outage doesn't fail the run.

The file list is read from the directory page each time it runs. Nothing is
hardcoded, so a new file is picked up without a code change.

**Reading the BLS documentation changed two answers.** `pr.period` is where
`Q05` turns out to be the annual average, not a fifth quarter — the single most
important fact here, and you only find it by reading the docs.

The readable labels question 2 asks for come from `pr.series`, which carries a
`series_title` written by BLS: *Employment for the Manufacturing sector,
% Change same quarter 1 year ago*. `sector_name` and `measure_text` are kept
alongside it as separate columns, because they are useful to filter on even
though the title already reads well on its own.

Observations come from `pr.data.1.AllData`, not `pr.data.0.Current`. `Current`
only covers recent years; question 2 needs each series' full history.

### Re-running safely

"Skip if the file already exists" isn't enough, because `pr.data.0.Current`
changes in place. So ingestion keeps a manifest — a Delta table with one row
per file holding `etag`, `last_modified`, `content_sha256`, `size_bytes`,
`ingested_at` and `status`.

For each file:

1. `HEAD` the URL — asks about the file without downloading it
2. Headers unchanged since last time → skip
3. Headers changed → download, hash it, compare to the stored hash
   - same hash → update the headers only, **don't rewrite the file**
   - different hash → write to the Volume, update the row
4. In the manifest but no longer on the server → mark `removed`

**The headers are a hint; the hash is the proof.** BLS builds its ETag from the
publish batch, not from the file contents — `pr.data.1.AllData` and `pr.series`
came back with the same ETag, `"0d4ccef9f3bdd1:0"`. So when BLS republishes,
the ETag changes even on files that didn't. Step 3's hash check is what stops
the whole batch being downloaded again for nothing.

Removed files are marked, not deleted. A file disappearing upstream is worth
knowing about, and deleting the row throws that away.

### Bronze / Silver / Gold

**Bronze** stores what the source sent, everything as text, one table per file.
No casting, no filtering. If someone later asks what BLS published on a given
day, Bronze and the manifest have the answer.

Two exceptions, both about names rather than data. BLS pads its **column
headers** to a fixed width inside a tab-separated file — `series_id␣␣␣␣␣`,
`␣␣␣␣␣value` — and Spark can't find the column at all. The population API
returns `Nation ID`, which Delta rejects because of the space. Both are cleaned
at Bronze, because the padding is how the file is formatted, not part of the
data. Only the names change; the values are untouched.

**Silver** trims, types and checks. The `series_id` *values* are padded too —
without trimming, `WHERE series_id = 'PRS30006032'` returns nothing and
question 3 quietly has no answer. Types are cast on purpose rather than
guessed. The expectations live here because this is the first layer that makes
a claim about the data.

**Gold** answers the three questions, one table each.

### SQL or PySpark

PySpark is the primary version and builds the Gold tables; Spark SQL is the
alternative, in `notebooks/03_alternatives`. They don't just look alike — that
notebook joins the SQL result for question 2 against the PySpark Gold table and
counts the rows that disagree. The answer is zero.

PySpark is primary because everything else is Python — the ingestion, the TSV
reader, the expectations — and question 2's window function is easier to write
there.

SQL is clearer for plain aggregation: question 1 in SQL is four lines and needs
no explanation. On a team where analysts contribute I'd flip the default — SQL
for aggregations, PySpark for anything with steps or a helper function.

---

## Trade-offs

**Schema drift.** The expectations are the alarm: schema and not-null checks
stop the run rather than dropping rows, because on data like this a broken key
means the source changed shape. The file *count* is deliberately not checked —
that would contradict the requirement to handle files being added or removed,
so it logs a warning instead. For a real client: a written contract per file,
versioned with the code, alerting on drift instead of finding out when a cast
turns everything into nulls.

**Data volume.** The whole folder is about 4 MB. There's no partitioning, no
Z-ordering and no incremental read, because at this size they'd do nothing —
and I'd rather say that than imply a scale story that isn't there. At 100×:
Bronze becomes Auto Loader over the Volume, Silver becomes incremental on a
scan date, and the small lookup tables stay as they are. The manifest doesn't
change — it already works file by file, on content.

**Cost.** The manifest isn't a cost saving at this size and I won't pretend it
is; it's there for correctness. Serverless on a triggered pipeline is right
here — seconds of work, on demand, nothing left running. For a real client the
first step is **attribution before optimisation**: tag spend so it lands on a
team, because a cost nobody owns is a cost nobody reduces. After that, savings
usually come from a handful of expensive jobs rather than from everything being
slightly wasteful.

**Access control.** A read-only analyst using Gold gets exactly that:

```sql
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA workspace.rearc TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc.gold_population_stats TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc.gold_series_best_year TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc.gold_prs30006032_q01  TO `analysts`;
```

Nothing on Bronze, Silver, the Volume or the manifest. Free Edition is
single-user, so I couldn't add a second person to check the grants actually
behave this way — the SQL is right and the intent is the point, but it's
untested against a real second account. For a real client these would come from
a group synced out of the identity provider, and I'd treat the Volume as the
sensitive one: raw files are where unintended things usually hide.

**Monitoring.** Expectation pass rates are the first signal. The manifest is a
second: a run where every file is `skipped` means the source hasn't moved,
which is normal for a monthly release and a problem if it carries on past a
publication date. The gap, and the first thing I'd add: **a check that each
Silver table has more than zero rows.** The retrospective explains why.

---

## Retrospective

### The hardest part wasn't technical

**`Q05` is the annual average, not a fifth quarter.** `pr.period` lists
`Q01`–`Q04` as quarters and `Q05` as `AN AV`. Question 2 asks for the year with
the largest sum "across all its quarters" — and including `Q05` adds the
average of four numbers to those same four numbers.

The question is genuinely ambiguous. I excluded `Q05`, and instead of hiding
that inside a filter I carried `is_annual_average` through Silver so the
decision is visible where it's made. Ties go to the earliest year, so the
result is always the same.

The same habit caught a second one. Question 1 asks for "the standard
deviation" without saying which. Spark's `stddev` is the sample version, but
2013–2018 is the complete set of years the question names, not a sample of a
bigger group — so the population version is arguably the better default. Both
are in the output (4,158,441.04 sample, 3,796,119.94 population) and the reader
picks.

### The bug that worried me

`dlt.read()` reads the **last committed** version of a table, not the one being
built in the current run. So on a clean run every Silver table read an empty
Bronze, produced zero rows, and **the pipeline said it succeeded.**

`silver_population` hid it — its Bronze happened to exist from an earlier
attempt — so only some tables were empty, which looked like a data problem
rather than a framework one. `spark.read.table()` fixed it.

What makes it worth writing down is that it only happens on a **first run**. It
fixes itself on the second. I could have shipped this, watched it work every
time I ran it, and handed over something that produces empty Gold tables for
anyone deploying it fresh — which is exactly what a reviewer cloning this repo
is doing.

*Does it work on a clean deployment* is a different question from *does it
work*. And a green pipeline with empty tables is worse than a failed one,
because nothing tells you to look.

### The label I built, and the one BLS had already written

Question 2 asks for a label someone unfamiliar with BLS codes could understand
at a glance. I built one by joining `pr.series` to `pr.sector` and
`pr.measure`, which produced *Employment — Manufacturing*.

Then I looked at the output. `PRS30006011`, `012`, `013` and `014` all carried
that identical label. They are four different series — percent change on the
year, percent change on the quarter, an index, and a level in millions of jobs
— separated by `duration_code`, which my label had thrown away.

`pr.series` already has a `series_title` column written by BLS that
distinguishes them properly. I had read enough of the documentation to find the
lookup tables and not enough to notice the answer was sitting in the file I was
already reading. The label now comes from `series_title`; the components stay
as separate columns because they are useful to filter on.

It also only surfaced because I looked at the rows rather than the row count.
283 labelled series is a number that looks correct whether or not the labels
mean anything.

### Smaller things that cost time

- **The padding appears twice** — in the headers and in the values, needing
  different fixes in different places. Padded headers make the column
  impossible to find; padded values make a filter return nothing while looking
  completely correct.
- **The source changed while I was working on it.** `pr.data.1.AllData` was
  3,239,525 bytes with a September date during exploration and 3,882,812 bytes
  with a 5 November date by the time I ingested it. An accidental demonstration
  of why the manifest tracks content.
- **283 of 337 series have a best year.** The other 54 are listed in
  `pr.series` but have no non-`Q05` rows in the data file, checked with a
  left-anti join. The metadata describes more series than the data holds.
- **The tombstone pass assumed one source.** It compared the whole manifest
  against the BLS directory listing, which marked `population.json` removed on
  every run — it comes from the API and was never in that listing. Scoped it to
  rows whose `source_url` belongs to the BLS endpoint. A manifest covering more
  than one source has to know which source each row came from.
- **The population API has no 2020** — ACS didn't publish 1-year estimates that
  year. It's outside the range question 1 asks for, so I noted it rather than
  filling it in.

### Checked, and left alone

There are **no duplicates** on `(series_id, year, period)` across all 92,328
rows. I checked, then added an expectation to assert it, rather than adding a
`dropDuplicates()`.

A duplicate here would mean a bad publish or the same file landing twice. If
two rows share a key but hold different values, `dropDuplicates` picks one at
random and produces a wrong number with nothing to warn you. Deduplication
belongs where duplicates are expected — at-least-once streaming, CDC replay —
with a rule for which row wins. This isn't that, and a transform that does
nothing is worse than none at all, because it looks like a safety check and
isn't one.

---

## AI usage

I used Claude as a reference and a second opinion: drafting the ingestion
scaffold and stress-testing the manifest design — in particular whether the
ETag alone is enough to detect change, which it isn't, and working that out is
what led to the hash check — reading stack traces when the padded column names
and the duplicate decorator came up, and reviewing this document.

I wrote and own the pipeline, the expectations, the `Q05` and standard
deviation decisions, and the check that the two implementations agree. Every
line here is one I can explain.
