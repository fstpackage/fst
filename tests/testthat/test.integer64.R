
context("integer64 column")

suppressMessages(library(bit64))


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


# Prepare example
dtint64 <- data.frame(
  Int64 = as.integer64(sample(c(2345612345679, 1234567890, 8714567890), 100, replace = TRUE)))


test_that("Type integer64 issue #28", {
  expect_equal(class(dtint64$Int64), "integer64")

  # Write to fst
  fstwriteproxy(dtint64, "testdata/dt_int64.fst")

  # bit64 integer64 type preserved:
  dtint64_read <- fstreadproxy("testdata/dt_int64.fst")

  # bit64::integer64 type preserved:
  expect_equal(class(dtint64_read$Int64), "integer64")
  expect_identical(dtint64, dtint64_read)
})


test_that("Type integer64 with compression", {
  # Write to fst
  fstwriteproxy(dtint64, "testdata/dt_int64.fst", 95)

  dtint64_read <- fstreadproxy("testdata/dt_int64.fst")

  # bit64::integer64 type preserved:
  expect_equal(class(dtint64_read$Int64), "integer64")
  expect_identical(dtint64, dtint64_read)
})


test_that("Type integer64 with compression as data.table", {

  setDT(dtint64)
  fstwriteproxy(dtint64, "testdata/dt_int64.fst", 95)  # Write to fst

  dtint64_read <- fstreadproxy("testdata/dt_int64.fst", as.data.table = TRUE)

  # bit64::integer64 type preserved:
  expect_equal(class(dtint64_read$Int64), "integer64")
  expect_identical(dtint64, dtint64_read)
})
