test_that(".augment_vars_for_pnadc_c adds IBGE columns for checklist outputs", {
  ns <- asNamespace("datazoom.social")
  aug <- get(".augment_vars_for_pnadc_c", envir = ns)
  out <- aug(
    vars = c("V2009", "V1028", "V2001"),
    output_vars = c("rendimento_habitual_real", "sexo", "formal"),
    ensure_pnadc_vars = TRUE,
    raw_data = FALSE
  )
  expect_true(all(c("VD4019", "V2007", "VD4002", "VD4009", "VD4012") %in% out$vars))
  expect_true(length(out$added) > 0)
})

test_that(".augment_vars_for_pnadc_c is a no-op when disabled or vars is NULL", {
  ns <- asNamespace("datazoom.social")
  aug <- get(".augment_vars_for_pnadc_c", envir = ns)
  out1 <- aug(
    vars = c("V2009"),
    output_vars = NULL,
    ensure_pnadc_vars = FALSE,
    raw_data = FALSE
  )
  expect_identical(out1$vars, c("V2009"))
  expect_length(out1$added, 0)

  out2 <- aug(
    vars = NULL,
    output_vars = c("sexo"),
    ensure_pnadc_vars = TRUE,
    raw_data = FALSE
  )
  expect_null(out2$vars)
})

test_that("treat_pnadc creates rendimento_habitual_real and labor flags", {
  ns <- asNamespace("datazoom.social")
  tr <- get("treat_pnadc", envir = ns)
  df <- data.frame(
    UF = 35,
    V2007 = 1,
    V2009 = 30,
    VD3004 = 3,
    VD4019 = 1000,
    Habitual = 1.1,
    VD4002 = 1,
    VD4009 = 1,
    VD4012 = 1,
    VD4001 = 1,
    V4022 = 1,
    stringsAsFactors = FALSE
  )
  out <- suppressWarnings(tr(df))
  expect_true("rendimento_habitual_real" %in% names(out))
  expect_true(all(c("formal", "informal", "ocupado", "conta_propria") %in% names(out)))
})

test_that(".pnadc_implied_treated_outputs expands formal-related keeps", {
  ns <- asNamespace("datazoom.social")
  imp <- get(".pnadc_implied_treated_outputs", envir = ns)
  expect_true(all(c("ocupado", "desocupado") %in% imp(c("formal"))))
  expect_true("rendimento_habitual" %in% imp(c("rendimento_habitual_real")))
})
