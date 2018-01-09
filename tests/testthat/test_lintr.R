
context("code quality")

library(lintr)

# issues with lintr:
#   * is.data.table method from data.table not recognized
#   * RcppExports not excluded

test_that("Package Style", {
  lints <- with_defaults(line_length_linter = line_length_linter(120))
  lints <- lints[!(names(lints) %in%
    c("object_usage_linter", "camel_case_linter", "commas_linter", "multiple_dots_linter"))]

  codeFiles <- list.files(
    c("../../R", "../../tests"), "R$", full.names = TRUE, recursive = TRUE)

  # manualy remove RcppExports file and few generated files (e.g. by codecov())
  codeFiles <- codeFiles[!(codeFiles %in%
    c("../../R/RcppExports.R"))]

  # Calculate lintr results for all code files
  lintResults <- lintr:::flatten_lints(lapply(codeFiles, function(file) {
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

  if (length(lintResults) > 0) {
    lintResults <- sapply(lintResults,
      function(lintRes) {
        paste(lintRes$filename, " (", lintRes$line_number, "): ", lintRes$message)
      })

    print(lintResults)
  }

  expect_true(length(lintResults) == 0, paste(lintResults, sep = "\n", collapse = "\n"))
})
