
require(testthat)
require(data.table)
context("package interface")


source("helper.fstwrite.R")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


x <- data.table(A = 1:10, B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE))
# setkey(x, B)
# saveRDS(x, "keyedTable.rds")
xKey <- readRDS("keyedTable.rds")  # import keyed table to avoid memory leaks in data.table (LeakSanitizer)

test_that("From unkeyed data.table to data.table",
{
  fstwrite(x, "testdata/nokey.fst")

  y <- fstread("testdata/nokey.fst")
  expect_false(is.data.table(y))

  y <- fstread("testdata/nokey.fst", as.data.table = TRUE)
  expect_true(is.data.table(y))
})



test_that("From keyed data.table to data.table",
{
  fstwrite(xKey, "testdata/key.fst")

  y <- fstread("testdata/key.fst")
  expect_false(is.data.table(y))

  y <- fstread("testdata/key.fst", as.data.table = TRUE)
  expect_true(is.data.table(y))
  expect_equal(key(y), "B")

  expect_equal(xKey, y)
})
