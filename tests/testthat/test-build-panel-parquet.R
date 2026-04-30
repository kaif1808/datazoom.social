test_that("build_pnadc_panel_parquet matches in-memory panel counts", {
  skip_if_not_installed("arrow")

  df <- data.frame(
    UPA = c(1, 1, 1, 1, 1, 1),
    V1008 = c(10, 10, 10, 10, 10, 10),
    V1014 = c(1, 1, 1, 1, 1, 1),
    V2007 = c(1, 1, 1, 1, 1, 1),
    V20082 = c(1990, 1990, 1990, 1990, 1990, 1990),
    V20081 = c(5, 5, 5, 5, 5, 5),
    V2008 = c(10, 10, 10, 10, 10, 10),
    V2003 = c(1, 1, 1, 1, 1, 1),
    Ano = c(2019, 2019, 2019, 2019, 2020, 2020),
    Trimestre = c(1, 2, 3, 4, 1, 2),
    stringsAsFactors = FALSE
  )

  in_memory <- build_pnadc_panel(df, panel = "advanced")
  expected_id_ind <- sum(!is.na(in_memory$id_ind))
  expected_id_rs <- sum(!is.na(in_memory$id_rs))

  in_dir <- file.path(tempdir(), "pnadc_test_in")
  out_dir <- file.path(tempdir(), "pnadc_test_out")
  unlink(in_dir, recursive = TRUE, force = TRUE)
  unlink(out_dir, recursive = TRUE, force = TRUE)
  dir.create(in_dir, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(df, sink = file.path(in_dir, "panel.parquet"))

  res <- build_pnadc_panel_parquet(
    input = in_dir,
    panel = "advanced",
    output_path = out_dir,
    backend = "arrow",
    single_file = TRUE,
    overwrite = TRUE
  )

  expect_identical(res$backend, "arrow")
  expect_true(file.exists(file.path(out_dir, "pnadc_matched.parquet")))

  out <- arrow::read_parquet(file.path(out_dir, "pnadc_matched.parquet"))
  expect_equal(sum(!is.na(out$id_ind)), expected_id_ind)
  expect_equal(sum(!is.na(out$id_rs)), expected_id_rs)
})

test_that("load_pnadc exposes parquet-native controls", {
  fml <- names(formals(load_pnadc))
  expect_true(all(c("panel_mode", "panel_backend") %in% fml))
})
