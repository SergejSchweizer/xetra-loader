# Polars feature-plan benchmark

## Contents

- [Purpose and method](#purpose-and-method)
- [Reproducible command](#reproducible-command)
- [Measured baseline](#measured-baseline)
- [Interpretation](#interpretation)

## Purpose and method

This microbenchmark compares the current global Polars window plan with
separate per-instrument Polars plans on the same deterministic synthetic
adjusted-close and volume input. It measures feature calculation only; it does
not include EODHD requests, lake I/O, PostgreSQL refresh/publication, or cron
runtime.

Before timing, both candidates are checked against the independent feature
reference. Each candidate is then warmed up and run for five timed rounds; the
reported runtime is the median. The final outputs are also compared row by row
within a `1e-12` tolerance.

## Reproducible command

```bash
.venv/bin/python scripts/quality/benchmark_features.py \
  --rows 100000 --instruments 100 --rounds 5 --cores 6
```

The script sets `POLARS_MAX_THREADS` before importing Polars. The input contains
1,000 observations per instrument.

## Measured baseline

Measured on **2026-10-21** in the repository environment:

| Environment or result | Value |
| --- | ---: |
| CPU | Intel Pentium Gold 8505, 6 logical CPUs available |
| Python | 3.14.7 |
| Polars | 2.0.0 |
| Input rows / instruments | 100,000 / 100 |
| Polars thread budget / timed rounds | 6 / 5 |
| Global window-plan median | 0.014599 s |
| Per-instrument-plan median | 0.208243 s |
| Global-plan speedup | 14.26× |
| Independent oracle validation | PASS |
| Candidate equivalence | PASS |

The measurements above are medians from one local run. They are a reproducible
baseline, not a hardware-independent latency guarantee.

## Interpretation

For this workload and host, one global lazy window plan is substantially
faster than eagerly splitting the frame and executing a plan for every
instrument. The result supports retaining the global plan. It is not evidence
about total weekly sync duration or PostgreSQL materialized-view refresh time;
those require separate end-to-end measurements.
