
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


test_that("Read meta of uncompressed file", {
  fstwriteproxy(x, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrofrows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnnames, c("A", "B"))
  expect_equal(y$columntypes, c(8, 10))
})


test_that("Read meta of compressed file", {
  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrofrows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnnames, c("A", "B"))
  expect_equal(y$columntypes, c(8, 10))
})


test_that("Read meta of sorted file", {
  fstwriteproxy(xKey, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")
  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrofrows, 10)
  expect_equal(y$keys, "B")
  expect_equal(y$columnnames, c("A", "B"))
  expect_equal(y$columntypes, c(8, 10))
})
