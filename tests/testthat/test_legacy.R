
context("legacy format")


test_that("Read legacy format", {
  expect_error(
    dt <- read_fst("datasets/legacy.fst", old_format = TRUE),
    "Parameter old_format is depricated"
  )

  expect_error(
    metadata_fst("datasets/legacy.fst"),
    "File header information does not contain the fst format marker"
  )
})


test_that("0.9.8-0.9.18.fst", {

  # BigEndian systems will fail this test
  skip_on_cran()
  skip_on_ci()

  expect_equal(
    readRDS("datasets/test-set.rds"),
    read_fst("datasets/0.9.8-0.9.18.fst")
  )
})
