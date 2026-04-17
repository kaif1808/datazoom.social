# Plan: Memory-Efficient Column Selection in `load_pnadc()`

## Context

Loading 24 quarters (2015Q1–2020Q4) of PNADC panel data crashes R because `PNADcIBGE::get_pnadc()` always returns ~233 structural columns regardless of the `vars` argument, and `list_rbind()` combines all of them — 24 quarters × 233+ columns × millions of rows — before panel matching. The fix is to trim each quarter's dataframe down to only needed columns immediately after per-quarter processing, inside the `map2` loop, before the expensive `list_rbind()` + `build_pnadc_panel()` phases.

## Data flow

```
map2 loop (per quarter)
  ├── get_pnadc()           → ~233 cols, millions of rows
  ├── treat_pnadc() [opt]   → renamed/derived cols added
  ├── as.numeric() block
  ├── [NEW] column trim     → keep only cols_to_keep
  │   └── uses dplyr::select(any_of(...))
  └── df$Ano / df$Trimestre → added AFTER trim (safe)
         │
         ▼
list_rbind()               → 24 small dfs, not 24 fat dfs
         │
         ▼
build_pnadc_panel()        → needs only panel ID cols (already kept)
         │
         ▼
write panel files
```

## File to modify

**`R/load_pnadc.R`** — single file, targeted changes at four locations.

---

## Change 1 — Function signature

Add `output_vars = NULL` as the last named parameter (line ~72–75):

```r
load_pnadc <- function(save_to = getwd(), years,
                       quarters = 1:4, panel = "advanced",
                       raw_data = FALSE, save_options = c(TRUE, TRUE),
                       vars = NULL, output_vars = NULL) {
```

## Change 2 — Roxygen `@param` (insert after the `@param vars` block)

```r
#' @param output_vars A \code{character} vector of column names to retain
#'   after each quarter is processed (post-treatment names when
#'   \code{raw_data = FALSE}). Reduces memory by discarding all other columns
#'   before quarters are combined and panel matching runs. Panel identification
#'   columns (\code{UPA}, \code{V1008}, \code{V2007}, \code{V20082},
#'   \code{V20081}, \code{V2008}; plus \code{V2003} for \code{"advanced"})
#'   and structural columns (\code{UF}, \code{Habitual}, \code{ID_DOMICILIO},
#'   \code{V1014}) are always kept. Use \code{NULL} (default) to retain all
#'   columns (original behaviour).
```

## Change 3 — `param` list (optional, for clarity)

If `load_pnadc` stores arguments in a `param` list before the `map2` call, add:

```r
param$output_vars <- output_vars
```

This is optional — `output_vars` is already in scope inside the lambda as a closure variable.

## Change 4 — Column trim block (inside `map2` callback)

**Placement:** after the `if (!raw_data) { treat_pnadc(...); as.numeric(...) }` block, and **before** `df$Ano <- year` (which adds `Ano` as a new column and still works on the trimmed df).

Determine which panel match columns must be kept based on the panel mode in effect for this call. The exact names of `panel_required_basic` and `panel_required_advanced` character vectors should match whatever constants are already defined in the file (search for `V1008` or `UPA` to locate them):

```r
panel_match_cols <- if (param$panel == "none") {
  character(0)
} else if (param$panel == "advanced") {
  panel_required_advanced   # e.g. c("UPA","V1008","V2007","V20082","V20081","V2008","V2003")
} else {
  panel_required_basic      # e.g. c("UPA","V1008","V2007","V20082","V20081","V2008")
}

if (!is.null(output_vars)) {
  cols_to_keep <- unique(c(
    "UF", "Habitual", "ID_DOMICILIO", "V1014",
    panel_match_cols,
    output_vars,
    if (!is.null(vars)) vars else character(0)
  ))
  df <- df %>% dplyr::select(dplyr::any_of(cols_to_keep))
}
```

`any_of()` silently skips any name in `cols_to_keep` that isn't present in `df`, so raw PNADC column names in `vars` that were renamed by `treat_pnadc()` won't cause an error.

**Note on `panel_required_*` names:** these must match whatever symbol the file already uses for the panel identification column lists. Search for `UPA` or `V1008` in `load_pnadc.R` or any sourced helper to find the correct names before implementing.

---

## Verification

| Test | How | Expected |
|---|---|---|
| No regression | Run with `output_vars = NULL`, any year/quarter set | Output identical to current behaviour |
| Memory relief | Run 24 quarters with a small `output_vars` vector | Does not crash; peak memory drops significantly |
| Column presence | Check names of output panel files | Contains `output_vars` + structural + panel ID cols only |
| `panel = "none"` | Run with `panel = "none"` and `output_vars` set | Works; no panel match cols in output (empty set) |
| `raw_data = TRUE` | Run with raw column names in `output_vars` | Raw columns preserved correctly |
| Missing col name | Include a non-existent name in `output_vars` | Silently skipped, no error |

Example call for memory test:
```r
load_pnadc(
  years = 2015:2020, quarters = 1:4, panel = "advanced",
  vars = c("V1028"),
  output_vars = c("faixa_idade", "sexo", "faixa_educ",
                  "rendimento_habitual_real", "formal",
                  "conta_propria", "informal",
                  "ocupado", "desocupado", "fora_forca_trab")
)
```

# Plan: Memory-Efficient Column Selection in `load_pnadc()`

## Context

When loading 24 quarters (2015Q1–2020Q4) of PNADC panel data, `load_pnadc()` crashes R because `PNADcIBGE::get_pnadc()` always returns ~233 structural columns regardless of the `vars` argument, and the full column set for all quarters is held in memory simultaneously before panel matching. The user only needs a subset of columns for their final output (defined in `PNAD-C_REQUIRED_VARS.md`) and wants the function to drop unnecessary columns early — before the expensive `list_rbind()` + `build_pnadc_panel()` phases.

**Root cause:** `list_rbind(source_files)` at line 213 combines 24 quarters × 233+ columns × millions of rows into one dataframe. `build_pnadc_panel()` then operates on all those columns even though it only needs ~9 of them.

## Approach

Add a new parameter `output_vars = NULL` to `load_pnadc()`. When provided, after each quarter is downloaded and optionally treated, immediately trim to only the columns needed going forward. This happens inside the `map2` loop (before rbinding), so each quarter in memory is small.

**Columns always preserved (regardless of `output_vars`):**
- `UF`, `Habitual`, `ID_DOMICILIO`, `V1014` — structural identifiers always needed
- `Ano`, `Trimestre` — added after treatment, so implicitly preserved
- Panel matching columns — required by `build_pnadc_panel()`:
  - basic: `UPA`, `V1008`, `V2007`, `V20082`, `V20081`, `V2008`
  - advanced (adds): `V2003`

**Additional columns preserved when `output_vars` is set:**
- Everything listed in `output_vars` (post-treatment column names, e.g. `"faixa_idade"`, `"sexo"`, `"rendimento_habitual_real"`)
- Everything listed in `vars` (raw PNADC column names that survive treatment, e.g. `"V1028"`, `"V2001"`)

`dplyr::select(any_of(...))` is used so missing columns are silently skipped rather than erroring.

## File to Modify

**`R/load_pnadc.R`** — single file, targeted changes:

### 1. Function signature (line 72–75)
Add `output_vars = NULL` parameter:
```r
load_pnadc <- function(save_to = getwd(), years,
                       quarters = 1:4, panel = "advanced",
                       raw_data = FALSE, save_options = c(TRUE, TRUE),
                       vars = NULL, output_vars = NULL) {
```

### 2. Roxygen `@param` block (insert after `@param vars` block, around line 52)
```r
#' @param output_vars A \code{character} vector of column names to retain in
#'   the dataset after each quarter is processed (post-treatment if
#'   \code{raw_data = FALSE}). Reduces memory by discarding all other columns
#'   before quarters are combined and panel matching is run. Panel
#'   identification columns (\code{UPA}, \code{V1008}, \code{V2007},
#'   \code{V20082}, \code{V20081}, \code{V2008}, and \code{V2003} for
#'   \code{"advanced"}) and structural columns (\code{UF}, \code{Habitual},
#'   \code{ID_DOMICILIO}, \code{V1014}) are always kept regardless of this
#'   argument. Use \code{NULL} (the default) to retain all columns (original
#'   behaviour).
```

### 3. Column-selection block (insert inside `map2` callback, after treat_pnadc / as.numeric block, before `df$Ano <- year`)

Determine the panel match columns to always keep:
```r
panel_match_cols <- if (param$panel == "none") character(0)
                   else if (param$panel == "advanced") panel_required_advanced
                   else panel_required_basic
```

Then after the if/else for raw_data treatment, add:
```r
# Trim to memory-efficient column subset when output_vars is specified
if (!is.null(output_vars)) {
  cols_to_keep <- unique(c(
    "UF", "Habitual", "ID_DOMICILIO", "V1014",
    panel_match_cols,
    output_vars,
    if (!is.null(vars)) vars else character(0)
  ))
  df <- df %>% dplyr::select(dplyr::any_of(cols_to_keep))
}
```

The `df$Ano <- year` and `df$Trimestre <- quarter` assignments that immediately follow still work because they add new columns to the (now-trimmed) df.

### 4. Placement in `param` list (optional clarity)
Store `output_vars` in the param list so it's accessible throughout:
```r
param$output_vars <- output_vars
```
(Alternatively, close over `output_vars` directly — it's in scope inside the map2 lambda.)

## Verification

1. **Small test (4 quarters):** Run with `output_vars = NULL` → identical output to current behaviour (no regression).
2. **Memory test (24 quarters):** Run with `output_vars = c("faixa_idade", "sexo", "faixa_educ", "rendimento_habitual_real", "formal", "conta_propria", "informal", "ocupado", "desocupado", "fora_forca_trab")` for 2015Q1–2020Q4 — should no longer crash.
3. **Column check:** Confirm final panel files contain the `output_vars` columns + structural cols + panel ID columns (`id_ind`, `id_rs`), and no extra 200+ structural columns.
4. **`panel = "none"`:** Confirm it still works (panel_match_cols is empty, no harm).
5. **`raw_data = TRUE`:** Confirm raw column names in `output_vars` are kept correctly.
