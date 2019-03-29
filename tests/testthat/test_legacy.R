
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
