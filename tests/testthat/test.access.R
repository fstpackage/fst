
context("access")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}

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


test_that("Columns need to be of type character", {
  fstwriteproxy(data.frame(A = 1:10), "testdata/bla.fst")
  expect_error(fstreadproxy("testdata/bla.fst", 2), "Parameter 'columns' should be a character vector of column names")
})


test_that("Columns need to be of type character", {
  expect_error(fstreadproxy("testdata/bla.fst", to = c(1, 2)), "Parameter 'to' should have a numerical value")
})


test_that("Old read and write interface still functional", {
  x <- fstreadproxy("testdata/bla.fst")
  y <- read.fst("testdata/bla.fst")
  write.fst(y, "testdata/bla.fst")

  expect_identical(x, y)
})


test_that("package can be unloaded cleanly", {
  detach("package:fst", unload = TRUE)
  library(fst)
})
