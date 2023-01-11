
context("metadata")

require(data.table)

require_extensions <- function() {
  requireNamespace("nanotime", quietly = TRUE) && requireNamespace("bit64", quietly = TRUE)
}

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


posix_i <- as.integer(as.POSIXct("2001-02-03")) + 1L:5L
class(posix_i) <- c("POSIXct", "POSIXt")

date_i <- as.integer(Sys.Date() + 1:10)
class(date_i) <- "Date"

difftime <- (Sys.time() + 1:10) - Sys.time()
difftime_int <- difftime
mode(difftime_int) <- "integer"


# Sample data
x <- data.table(
  A = 1:10,
  B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  C = sample(1:100, 10) / 100,
  D = Sys.Date() + 1:10,
  E = sample(LETTERS, 10),
  F = factor(sample(LETTERS, 10)),
  G = as.POSIXct("2001-02-03") + 1:10,
  H = posix_i,
  I = date_i,
  J = as.raw(sample(0:255, 10)),
  K = ordered(sample(LETTERS, 10)),
  L = difftime,
  M = difftime_int,
  N = as.ITime(Sys.time() + 1:10)
)


if (require_extensions()) {
  x$O <- bit64::as.integer64(101:110)  # nolint
  x$P <- nanotime::nanotime(2:11)  # nolint
}


test_that("v0.7.2 interface still works", {
  fstwriteproxy(x, "testdata/meta.fst")

  x <- fst.metadata("testdata/meta.fst")

  expect_true(!is.null(x))
})


test_that("format contains fst magic value", {
  zz <- file("testdata/meta.fst", "rb")  # open file in binary mode
  header_hash <- readBin(zz, integer(), 12)
  close(zz)

  expect_equal(header_hash[3], 1)  # fst format version (even numbers are dev versions)
  expect_equal(header_hash[12], 1346453840)  # fst magic number
  expect_equal(header_hash[5], 0)  # free bytes
  expect_equal(header_hash[6], 0)  # free bytes
})


test_that("Read meta of uncompressed file", {
  fstwriteproxy(x, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(basename(y$path), basename("meta.fst"))
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnNames, LETTERS[1:16][seq_len(ncol(x))])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 5, 2, 3, 5, 4, 4, 8, 3, 5, 4, 4, 7, 7)[seq_len(ncol(x))])
  expect_equal(y$columnTypes, c(5, 15, 10, 11, 2, 3, 12, 6, 8, 18, 4, 13, 7, 9, 16, 17)[seq_len(ncol(x))])
})


test_that("Read meta of compressed file", {
  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(basename(y$path), "meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, NULL)
  expect_equal(y$columnNames, LETTERS[1:16][seq_len(ncol(x))])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 5, 2, 3, 5, 4, 4, 8, 3, 5, 4, 4, 7, 7)[seq_len(ncol(x))])
  expect_equal(y$columnTypes, c(5, 15, 10, 11, 2, 3, 12, 6, 8, 18, 4, 13, 7, 9, 16, 17)[seq_len(ncol(x))])
})


test_that("Print meta data without keys", {

  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")
  res <- capture_output(print(fstmetaproxy("testdata/meta.fst")))

  if (require_extensions()) {
    expect_equal(res, paste(
      "<fst file>\n10 rows, 16 columns (meta.fst)\n",
      "* 'A': integer",
      "* 'B': logical",
      "* 'C': double",
      "* 'D': Date",
      "* 'E': character",
      "* 'F': factor",
      "* 'G': POSIXct",
      "* 'H': POSIXct",
      "* 'I': IDate",
      "* 'J': raw",
      "* 'K': ordered factor",
      "* 'L': difftime",
      "* 'M': difftime",
      "* 'N': ITime",
      "* 'O': integer64",
      "* 'P': nanotime",
      sep = "\n"))
  } else {
    expect_equal(res, paste(
      "<fst file>\n10 rows, 14 columns (meta.fst)\n",
      "* 'A': integer",
      "* 'B': logical",
      "* 'C': double",
      "* 'D': Date",
      "* 'E': character",
      "* 'F': factor",
      "* 'G': POSIXct",
      "* 'H': POSIXct",
      "* 'I': IDate",
      "* 'J': raw",
      "* 'K': ordered factor",
      "* 'L': difftime",
      "* 'M': difftime",
      "* 'N': ITime",
      sep = "\n"))
  }
})


# raw sorting not supported
x$J <- NULL  # nolint


test_that("Read meta of sorted file", {

  z <- copy(x)
  setkey(z, B, C)
  fstwriteproxy(z, "testdata/meta.fst")
  y <- fstmetaproxy("testdata/meta.fst")

  expect_equal(basename(y$path), "meta.fst")
  expect_equal(y$nrOfRows, 10)
  expect_equal(y$keys, c("B", "C"))
  expect_equal(y$columnNames, LETTERS[c(1:9, 11:16)][seq_len(ncol(x))])
  expect_equal(y$columnBaseTypes, c(4, 6, 5, 5, 2, 3, 5, 4, 4, 3, 5, 4, 4, 7, 7)[seq_len(ncol(x))])
  expect_equal(y$columnTypes, c(5, 15, 10, 11, 2, 3, 12, 6, 8, 4, 13, 7, 9, 16, 17)[seq_len(ncol(x))])
})


test_that("Print meta data with keys", {

  z <- copy(x)
  setkey(z, D, K, B)
  fstwriteproxy(z, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")
  res <- capture_output(print(fstmetaproxy("testdata/meta.fst")))

  if (require_extensions()) {
    expect_equal(res, paste(
      "<fst file>\n10 rows, 15 columns (meta.fst)\n",
      "* 'D': Date (key 1)",
      "* 'K': ordered factor (key 2)",
      "* 'B': logical (key 3)",
      "* 'A': integer",
      "* 'C': double",
      "* 'E': character",
      "* 'F': factor",
      "* 'G': POSIXct",
      "* 'H': POSIXct",
      "* 'I': IDate",
      "* 'L': difftime",
      "* 'M': difftime",
      "* 'N': ITime",
      "* 'O': integer64",
      "* 'P': nanotime",
      sep = "\n"))
  } else {
    expect_equal(res, paste(
      "<fst file>\n10 rows, 13 columns (meta.fst)\n",
      "* 'D': Date (key 1)",
      "* 'K': ordered factor (key 2)",
      "* 'B': logical (key 3)",
      "* 'A': integer",
      "* 'C': double",
      "* 'E': character",
      "* 'F': factor",
      "* 'G': POSIXct",
      "* 'H': POSIXct",
      "* 'I': IDate",
      "* 'L': difftime",
      "* 'M': difftime",
      "* 'N': ITime",
      sep = "\n"))
  }
})


test_that("Print meta data with keys and unordered columns", {

  skip_if_not(require_extensions())

  colnames(x) <- LETTERS[seq.int(ncol(x), 1)]

  setkey(x, L, F, N)  # nolint
  fstwriteproxy(x, "testdata/meta.fst", compress = 100)
  y <- fstmetaproxy("testdata/meta.fst")
  res <- capture_output(print(fstmetaproxy("testdata/meta.fst")))

  # note that columns are printed in original order (except keys)
  expect_equal(res, paste(
    "<fst file>\n10 rows, 15 columns (meta.fst)\n",
    "* 'L': Date (key 1)",
    "* 'F': ordered factor (key 2)",
    "* 'N': logical (key 3)",
    "* 'O': integer",
    "* 'M': double",
    "* 'K': character",
    "* 'J': factor",
    "* 'I': POSIXct",
    "* 'H': POSIXct",
    "* 'G': IDate",
    "* 'E': difftime",
    "* 'D': difftime",
    "* 'C': ITime",
    "* 'B': integer64",
    "* 'A': nanotime",
    sep = "\n"))
})
