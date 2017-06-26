
context("integer64 column")

require(bit64)

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


# Prepare example
dtInt64 <- data.table(
  Int64 = as.integer64.double(sample(c(2345612345679, 1234567890, 8714567890), 100, replace = TRUE)))


test_that("Type integer64 issue #28",
{
  expect_equal(class(dtInt64$Int64), "integer64")

  # Write to fst
  write.fst(dtInt64, "dt_int64.fst")

  # bit64 integer64 type preserved:
  dtInt64_read <- read.fst("dt_int64.fst", as.data.table = TRUE)

  # bit64::integer64 type preserved:
  expect_equal(class(dtInt64_read$Int64), "integer64")
  expect_identical(dtInt64, dtInt64_read)
})


test_that("Type integer64 with compression",
{
  # Write to fst
  write.fst(dtInt64, "dt_int64.fst", 95)

  dtInt64_read <- read.fst("dt_int64.fst", as.data.table = TRUE)

  # bit64::integer64 type preserved:
  expect_equal(class(dtInt64_read$Int64), "integer64")
  expect_identical(dtInt64, dtInt64_read)
})
