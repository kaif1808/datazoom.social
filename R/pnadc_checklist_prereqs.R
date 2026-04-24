# Internal helpers: IBGE columns needed so `treat_pnadc()` can build variables
# referenced in PNAD-C_REQUIRED_VARS.md (raw + pretreated-style outputs).

#' @noRd
#' @keywords internal
.pnadc_ibge_prerequisites_pnadc_c <- function() {
  unique(c(
    "VD4019",
    "VD4002",
    "VD4009",
    "VD4012",
    "VD4001",
    "V4022",
    "V2007",
    "V2009",
    "VD3004"
  ))
}

#' Map post-treatment column names (requested in `output_vars`) to IBGE inputs.
#' @noRd
#' @keywords internal
.pnadc_ibge_for_output_vars <- function(output_vars) {
  if (is.null(output_vars) || !length(output_vars)) {
    return(character(0))
  }
  out <- character(0)
  ov <- output_vars
  if (any(c("rendimento_habitual_real", "rendimento_habitual") %in% ov)) {
    out <- c(out, "VD4019")
  }
  if ("sexo" %in% ov) {
    out <- c(out, "V2007")
  }
  if ("faixa_idade" %in% ov) {
    out <- c(out, "V2009")
  }
  if ("faixa_educ" %in% ov) {
    out <- c(out, "VD3004")
  }
  if (any(c("ocupado", "desocupado", "formal", "informal") %in% ov)) {
    out <- c(out, "VD4002", "VD4009", "VD4012")
  }
  if ("fora_forca_trab" %in% ov) {
    out <- c(out, "VD4001")
  }
  if ("conta_propria" %in% ov) {
    out <- c(out, "VD4009", "VD4012", "V4022")
  }
  unique(out)
}

#' Union IBGE prerequisites into `vars` when trimming downloads (`vars` non-NULL).
#' @noRd
#' @keywords internal
.augment_vars_for_pnadc_c <- function(vars, output_vars, ensure_pnadc_vars, raw_data) {
  if (!isTRUE(ensure_pnadc_vars) || isTRUE(raw_data) || is.null(vars)) {
    return(list(vars = vars, added = character(0)))
  }
  want <- unique(c(
    .pnadc_ibge_prerequisites_pnadc_c(),
    .pnadc_ibge_for_output_vars(output_vars)
  ))
  added <- setdiff(want, vars)
  list(vars = unique(c(vars, want)), added = added)
}

#' Extra treated columns to retain when user requests a related output.
#' @noRd
#' @keywords internal
.pnadc_implied_treated_outputs <- function(output_vars) {
  if (is.null(output_vars) || !length(output_vars)) {
    return(character(0))
  }
  out <- character(0)
  if ("rendimento_habitual_real" %in% output_vars) {
    out <- c(out, "rendimento_habitual")
  }
  if (any(c("formal", "informal") %in% output_vars)) {
    out <- unique(c(out, "ocupado", "desocupado"))
  }
  setdiff(unique(out), output_vars)
}
