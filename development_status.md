## Development Status

### 2026-04-24
- Investigated report that `load_pnadc()` was not creating an obvious large matched output artifact.
- Confirmed panel matching still runs correctly with `panel = "advanced"` and `output_vars` trimming enabled.
- Updated `R/load_pnadc.R` parquet panel-save path to also write a consolidated file: `pnadc_matched.parquet`, in addition to the partitioned dataset directory `pnadc_panels/`.
- Validated output with a 2017Q1-2017Q2 run: consolidated file created with 1,140,692 rows.
- Executed full matched build for 2015Q1-2020Q4 using `panel = "advanced"` and `output_vars` trimming; produced `data/full_2015_2020/pnadc_matched.parquet` with 12,821,306 rows across 24 quarters.
- Removed temporary test-run artifacts from `data/` (`debug_*`, `test_2015_2017`, `pnadc_quarters`) while keeping persistent outputs (`full_2015_2020`) and packaged sample data (`pnad_sample.rda`).
- Patched `R/load_pnadc.R` parquet panel-save path to write one parquet file per panel directly under `pnadc_panels/` (memory-safe) and only attempt consolidated `pnadc_matched.parquet` in a `tryCatch`, warning instead of failing when memory is insufficient.
- PNAD-C checklist: added `R/pnadc_checklist_prereqs.R` with IBGE prerequisite mapping; `load_pnadc()` gains `ensure_pnadc_vars` (defaults to on when `raw_data = FALSE`) to union required inputs into `vars` when `vars` is not `NULL`, plus implied treated columns when trimming with `output_vars`, and a post-bind warning if requested `output_vars` are still missing.
- Added `testthat` to `DESCRIPTION` and tests under `tests/testthat/test-pnadc-prereqs.R` for augmentation, implied outputs, and `treat_pnadc()` smoke coverage.
