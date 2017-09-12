
context("metadata")

library(bit64, quietly = TRUE)
library(nanotime, quietly = TRUE)
library(data.table, quietly = TRUE)


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


posixI <- as.integer(as.POSIXct("2001-02-03")) + 1L:5L
class(posixI) <- c("POSIXct", "POSIXt")

dateI <- as.integer(Sys.Date() + 1:10)
class(dateI) <- "Date"

# Sample data
x <- data.table(
  A = 1:10,
  B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  C = sample(1:100, 10) / 100,
  D = Sys.Date() + 1:10,
  E = sample(LETTERS, 10),
  F = factor(sample(LETTERS, 10)),
  G = as.integer64(101:110),
  H = nanotime(2:11),
  I = as.POSIXct("2001-02-03") + 1:10,
  J = posixI,
  K = dateI,
  L = as.raw(sample(0:255, 10)),
  M = ordered(sample(LETTERS, 10)))


test_that("Read meta of uncompressed file", {
  fstwriteproxy(x, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnNames, LETTERS[1:13])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 5, 2, 3, 7, 7, 5, 4, 4, 8, 3))
  expect_equal(y$columnTypes, c(5, 11, 8, 9, 2, 3, 12, 13, 10, 6, 7, 15, 4))
})


test_that("Read meta of compressed file", {
  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnNames, LETTERS[1:13])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 5, 2, 3, 7, 7, 5, 4, 4, 8, 3))
  expect_equal(y$columnTypes, c(5, 11, 8, 9, 2, 3, 12, 13, 10, 6, 7, 15, 4))
})


test_that("Read meta of sorted file", {
  z <- x[, c(1:11, 13)]  # raw sorting not supported
  setkey(z, B, C)
  fstwriteproxy(z, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(y$path, "testdata/meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, c("B", "C"))
  expect_equal(y$columnNames, LETTERS[c(1:11, 13)])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 5, 2, 3, 7, 7, 5, 4, 4, 3))
  expect_equal(y$columnTypes, c(5, 11, 8, 9, 2, 3, 12, 13, 10, 6, 7, 4))
})


test_that("Print meta data", {
  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")
  res <- capture_output(print(fstmetaproxy("testdata/meta.fst")))

  expect_equal(res, paste(
    "<fst file>\n10 rows, 13 columns (testdata/meta.fst)\n",
    "* 'A': integer",
    "* 'B': logical",
    "* 'C': double",
    "* 'D': Date",
    "* 'E': character",
    "* 'F': factor",
    "* 'G': integer64",
    "* 'H': nanotime",
    "* 'I': POSIXct",
    "* 'J': POSIXct",
    "* 'K': IDate",
    "* 'L': raw",
    "* 'M': ordered factor", sep = "\n"))
})
