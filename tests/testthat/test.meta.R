
require(testthat)
require(data.table)
context("metadata")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}

# Sample data
x <- data.table(A = 1:10, B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE))
xKey <- readRDS("keyedTable.rds")  # import keyed table to avoid memory leaks in data.table (LeakSanitizer)


test_that("Read meta of uncompressed file",
{
  fstwrite(x, "testdata/meta.fst")
  y <- fst.metadata("testdata/meta.fst")

  expect_equal(y$Path, "testdata/meta.fst")
  expect_equal(y$NrOfRows, 10)
  expect_equal(y$Keys, NULL)
  expect_equal(y$ColumnNames, c("A", "B"))
  expect_equal(y$ColumnTypes, c(8, 10))
})


test_that("Read meta of compressed file",
{
  fstwrite(x, "testdata/meta.fst", compress = 100)
  y <- fst.metadata("testdata/meta.fst")

  expect_equal(y$Path, "testdata/meta.fst")
  expect_equal(y$NrOfRows, 10)
  expect_equal(y$Keys, NULL)
  expect_equal(y$ColumnNames, c("A", "B"))
  expect_equal(y$ColumnTypes, c(8, 10))
})


test_that("Read meta of sorted file",
{
  fstwrite(xKey, "testdata/meta.fst")
  y <- fst.metadata("testdata/meta.fst")
  expect_equal(y$Path, "testdata/meta.fst")
  expect_equal(y$NrOfRows, 10)
  expect_equal(y$Keys, "B")
  expect_equal(y$ColumnNames, c("A", "B"))
  expect_equal(y$ColumnTypes, c(8, 10))
})
