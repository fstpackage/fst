
context("access")


test_that("Reading non-existent file gives an error", {
  expect_error(fstreadproxy("AccessStore/non-existent.fst"))
})


test_that("Writing to incorrect path gives an error", {
  expect_error(fstwriteproxy(data.frame(A = 1:10), "AccessStore/A/non-existent.fst"))
})


test_that("Dataset needs at least one column", {
  expect_error(fstwriteproxy(data.frame(), "bla.fst"), "Your dataset needs at least one column")
})

test_that("Dataset needs at least one column", {
  expect_error(fstwriteproxy(data.frame(A = integer(0)), "bla.fst"), "The dataset contains no data")
})
