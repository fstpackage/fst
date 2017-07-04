
context("code quality")

require(lintr)

# issues with lintr:
#   * is.data.table method from data.table not recognized
#   * RcppExports not excluded
#
# if (requireNamespace("lintr", quietly = TRUE)) {
#   test_that("Package Style", {
#     lints <- with_defaults(line_length_linter = line_length_linter(120))
#     lints <- lints[!(names(lints) %in% c("object_usage_linter", "camel_case_linter", "commas_linter"))]
#
#     expect_lint_free(linters = lints)
#   })
# }
