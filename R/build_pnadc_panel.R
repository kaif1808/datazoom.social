#' Build PNADc Panel
#'
#' This function builds a panel dataset from PNADC data, indentifying households and individuals
#'
#' @param dat Data frame with PNADC data, sorted into a single panel.
#' @param panel A \code{character} with the type of panel identification. Use "none" for no paneling, "basic" for basic paneling, and "advanced" for advanced paneling.
#'
#' @return A modified dataset with added identifiers for household (\code{id_dom}) and individual (\code{id_ind} or \code{id_rs}) based on the chosen panel algorithm.
#'
#' @examplesIf interactive()
#' # Example usage:
#'
#' panel_data <- build_pnadc_panel(dat = pnad_sample, panel = "basic")
#'
#' @export
build_pnadc_panel <- function(dat, panel) {
  .validate_panel_choice(panel)
  .build_pnadc_panel_impl(dat = dat, panel = panel)
}

#' Build PNADc Panel Directly from Parquet
#'
#' Builds PNADC panel identifiers from parquet inputs without requiring callers
#' to manually materialize all data in memory first. This function can write
#' per-panel parquet outputs incrementally and optionally export a single merged
#' parquet file.
#'
#' @param input Character. Either a parquet dataset directory or parquet file.
#' @param panel Panel algorithm: \code{"none"}, \code{"basic"}, or
#'   \code{"advanced"}.
#' @param output_path Character. Destination directory for parquet outputs.
#' @param backend Character. Backend used for parquet-native processing:
#'   \code{"arrow"} (default) or \code{"duckdb"}.
#' @param single_file Logical. If \code{TRUE}, also writes one merged parquet
#'   file named \code{pnadc_matched.parquet} in \code{output_path}.
#' @param overwrite Logical. If \code{TRUE}, existing outputs are replaced.
#'
#' @return A list with output paths and row counts.
#' @export
build_pnadc_panel_parquet <- function(input,
                                      panel = "advanced",
                                      output_path,
                                      backend = c("arrow", "duckdb"),
                                      single_file = FALSE,
                                      overwrite = FALSE) {
  .validate_panel_choice(panel)
  backend <- match.arg(backend)
  if (!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  }
  if (backend == "duckdb") {
    return(.build_pnadc_panel_parquet_duckdb(
      input = input,
      panel = panel,
      output_path = output_path,
      single_file = single_file,
      overwrite = overwrite
    ))
  }
  .build_pnadc_panel_parquet_arrow(
    input = input,
    panel = panel,
    output_path = output_path,
    single_file = single_file,
    overwrite = overwrite
  )
}

.validate_panel_choice <- function(panel) {
  if (!panel %in% c("none", "basic", "advanced")) {
    stop("`panel` must be one of: 'none', 'basic', 'advanced'.", call. = FALSE)
  }
}

.build_pnadc_panel_impl <- function(dat, panel) {
  UPA <- V1008 <- V1014 <- id_dom <- V20082 <- V20081 <- V2008 <- V2007 <- NULL
  Ano <- Trimestre <- id_ind <- num_appearances <- V2003 <- id_rs <- NULL
  num_appearances_rs <- q_count_ind <- q_count_rs <- NULL

  if (panel == "none") {
    return(dat)
  }

  dat <- dat %>%
    dplyr::mutate(id_dom = dplyr::cur_group_id(), .by = c(UPA, V1008, V1014)) %>%
    dplyr::mutate(id_ind = dplyr::cur_group_id(), .by = c(id_dom, V20082, V20081, V2008, V2007)) %>%
    dplyr::add_count(id_ind, Ano, Trimestre, name = "num_appearances") %>%
    dplyr::mutate(id_ind = dplyr::case_when(num_appearances != 1 ~ NA_real_, .default = id_ind)) %>%
    dplyr::mutate(id_ind = dplyr::case_when(
      V2008 == 99 | V20081 == 99 | V20082 == 9999 ~ NA_real_,
      .default = id_ind
    ))

  if (panel == "advanced") {
    m <- suppressWarnings(max(dat$id_ind, na.rm = TRUE))
    if (!is.finite(m)) m <- 0
    dat <- dat %>%
      dplyr::mutate(id_rs = dplyr::cur_group_id() + m, .by = c(id_dom, V20081, V2008, V2003)) %>%
      dplyr::add_count(id_rs, Ano, Trimestre, name = "num_appearances_rs") %>%
      dplyr::mutate(id_rs = dplyr::case_when(num_appearances_rs != 1 ~ NA_real_, .default = id_rs)) %>%
      dplyr::mutate(id_rs = dplyr::case_when(V2008 == 99 | V20081 == 99 ~ NA_real_, .default = id_rs)) %>%
      dplyr::mutate(q_count_ind = dplyr::n_distinct(interaction(Ano, Trimestre)), .by = id_ind) %>%
      dplyr::mutate(q_count_rs = dplyr::n_distinct(interaction(Ano, Trimestre)), .by = id_rs) %>%
      dplyr::mutate(
        id_rs = dplyr::case_when(
          q_count_ind == 5 ~ id_ind,
          q_count_rs > q_count_ind & q_count_rs <= 5 ~ id_rs,
          TRUE ~ dplyr::coalesce(id_ind, id_rs)
        )
      )
  }

  dat$id_ind <- ifelse(
    is.na(dat$id_ind),
    NA_character_,
    paste0(as.hexmode(dat$V1014), as.hexmode(as.integer(dat$id_ind)))
  )
  if (panel == "advanced") {
    dat$id_rs <- ifelse(
      is.na(dat$id_rs),
      NA_character_,
      paste0(as.hexmode(dat$V1014), as.hexmode(as.integer(dat$id_rs)))
    )
  }
  dat
}

.build_pnadc_panel_parquet_arrow <- function(input, panel, output_path, single_file, overwrite) {
  V1014 <- NULL
  ds <- arrow::open_dataset(input, format = "parquet")
  panel_ids <- ds %>%
    dplyr::distinct(V1014) %>%
    dplyr::arrange(V1014) %>%
    dplyr::collect() %>%
    dplyr::pull(V1014)

  panels_dir <- file.path(output_path, "pnadc_panels")
  if (dir.exists(panels_dir) && isTRUE(overwrite)) {
    unlink(panels_dir, recursive = TRUE, force = TRUE)
  }
  dir.create(panels_dir, recursive = TRUE, showWarnings = FALSE)

  row_counts <- vector("list", length(panel_ids))
  for (i in seq_along(panel_ids)) {
    p <- panel_ids[[i]]
    message(sprintf("Compiling panel %s", p))
    dat <- ds %>% dplyr::filter(V1014 == p) %>% dplyr::collect()
    df <- .build_pnadc_panel_impl(dat = dat, panel = panel)
    panel_subdir <- file.path(panels_dir, paste0("V1014=", p))
    dir.create(panel_subdir, recursive = TRUE, showWarnings = FALSE)
    panel_file <- file.path(panel_subdir, paste0("panel_", p, ".parquet"))
    arrow::write_parquet(df, sink = panel_file)
    row_counts[[i]] <- data.frame(V1014 = p, rows = nrow(df))
  }

  if (isTRUE(single_file)) {
    matched_file <- file.path(output_path, "pnadc_matched.parquet")
    if (file.exists(matched_file) && isTRUE(overwrite)) {
      file.remove(matched_file)
    }
    panel_ds <- arrow::open_dataset(
      panels_dir,
      format = "parquet",
      partitioning = arrow::schema(V1014 = arrow::float64()),
      hive_style = TRUE
    )
    merged <- panel_ds %>% dplyr::collect()
    arrow::write_parquet(merged, sink = matched_file)
  }

  list(panels_dir = panels_dir, rows = dplyr::bind_rows(row_counts), backend = "arrow")
}

.build_pnadc_panel_parquet_duckdb <- function(input, panel, output_path, single_file, overwrite) {
  if (!requireNamespace("duckdb", quietly = TRUE) || !requireNamespace("DBI", quietly = TRUE)) {
    stop("`duckdb` and `DBI` are required for backend = 'duckdb'.", call. = FALSE)
  }

  out <- .build_pnadc_panel_parquet_arrow(
    input = input,
    panel = panel,
    output_path = output_path,
    single_file = FALSE,
    overwrite = overwrite
  )

  if (isTRUE(single_file)) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    in_glob <- file.path(out$panels_dir, "V1014=*", "panel_*.parquet")
    out_file <- file.path(output_path, "pnadc_matched.parquet")
    if (file.exists(out_file) && isTRUE(overwrite)) {
      file.remove(out_file)
    }
    sql <- sprintf(
      paste(
        "COPY (SELECT * FROM read_parquet('%s', hive_partitioning = false))",
        "TO '%s' (FORMAT parquet, COMPRESSION snappy);"
      ),
      gsub("'", "''", in_glob, fixed = TRUE),
      gsub("'", "''", out_file, fixed = TRUE)
    )
    DBI::dbExecute(con, sql)
  }

  out$backend <- "duckdb"
  out
}