
context("metadata")

library(bit64, quietly = TRUE)


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}

# Sample data
x <- data.table(
  A = 1:10,
  B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  C = sample(1:100, 10) / 100,
  D = Sys.Date() + 1:10,
  E = sample(LETTERS, 10),
  F = factor(sample(LETTERS, 10)))


test_that("Read meta of uncompressed file", {
  fstwriteproxy(x, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnNames, LETTERS[1:6])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 7, 2, 3))
})


test_that("Read meta of compressed file", {
  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnNames, LETTERS[1:6])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 7, 2, 3))
})


test_that("Read meta of sorted file", {
  setkey(x, B, C)
  fstwriteproxy(x, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")
  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, c("B", "C"))
  expect_equal(y$columnNames, LETTERS[1:6])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 7, 2, 3))
})
