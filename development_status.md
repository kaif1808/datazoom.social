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

### 2026-04-30
- Added `scripts/merge_panels_chunked.R`, a documented CLI script to merge Hive-partitioned PNADC panel parquet files using Arrow in a memory-safe chunked pipeline.
- Script includes robust argument parsing, dry-run mode, compression and batch/file sizing controls, and explicit partition schema handling to resolve the `V1014` type mismatch (`double` vs `int32`) when opening datasets.
- Validated against `data/full_2012_2025/pnadc_panels`: detects 13 panel files and 28,893,767 total rows in dry-run mode.
- Extended `scripts/merge_panels_chunked.R` with `--single_file TRUE` mode that writes one merged parquet file via DuckDB `COPY (SELECT * FROM read_parquet(...)) TO ...`.
- Kept default chunked Arrow dataset behavior; single-file mode now supports overwrite/compression and was validated with `data/full_2012_2025/pnadc_matched_single_test.parquet` (28,893,767 rows).
- Reworked panel internals with a backend-aware API: `build_pnadc_panel()` now delegates to shared staged internals, and new exported `build_pnadc_panel_parquet()` supports parquet-native execution with `backend = "arrow"` or `"duckdb"` plus optional single-file export.
- Updated `load_pnadc()` with `panel_mode` (`"in_memory"`/`"parquet_native"`) and `panel_backend` controls; parquet-native mode now avoids building the `identified_panels` in-memory list and runs panel identification from parquet partitions.
- Added tests in `tests/testthat/test-build-panel-parquet.R` to assert parity of identified counts between in-memory and parquet-native paths and to verify new `load_pnadc()` controls; full test suite passes (`PASS 14`).
- Updated docs via roxygen (`build_pnadc_panel_parquet.Rd`, `load_pnadc.Rd`) and expanded vignette `vignettes/BUILD_PNADC_PANEL.Rmd` with parquet-native workflow examples.
