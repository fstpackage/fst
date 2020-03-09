
context("code quality")

library(lintr)

# issues with lintr:
#   * is.data.table method from data.table not recognized
#   * RcppExports not excluded

test_that("Package Style", {

  # lintr throws a lot of valgrind warnings, so skip on CRAN for now
  skip_on_cran()

  # lintr has many new and updated lints from version 2 onwards
  major_lintr_version <- strsplit(as.character(packageVersion("lintr")), ".", fixed = TRUE)[[1]]

  if (as.integer(major_lintr_version[1]) >= 2) {
    lints <- with_defaults(
      line_length_linter = line_length_linter(120),
      cyclocomp_linter = cyclocomp_linter(37))
  } else {
    lints <- with_defaults(
      line_length_linter = line_length_linter(120))
  }

  lints <- lints[!(names(lints) %in%
    c("object_usage_linter", "camel_case_linter", "commas_linter", "multiple_dots_linter"))]

  code_files <- list.files(
    c("../../R", "../../tests"), "R$", full.names = TRUE, recursive = TRUE)

  # manualy remove RcppExports file and few generated files (e.g. by codecov())
  code_files <- code_files[!(code_files %in%
    c("../../R/RcppExports.R"))]

  # Calculate lintr results for all code files
  lint_results <- lintr:::flatten_lints(lapply(code_files, function(file) {
      if (interactive()) {
          message(".", appendLF = FALSE)
      }
      lint(file, linters = lints, parse_settings = FALSE)
  }))

  # newline
  if (interactive()) {
      message()
  }

  lint_output <- NULL

  if (length(lint_results) > 0) {
    lint_results <- sapply(lint_results,
      function(lint_res) {
        paste(lint_res$filename, " (", lint_res$line_number, "): ", lint_res$message)
      })

    print(lint_results)
  }

  expect_true(length(lint_results) == 0, paste(lint_results, sep = "\n", collapse = "\n"))
})
