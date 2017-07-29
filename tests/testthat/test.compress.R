
context("compression")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


# Tarred executables of Mozilla 1.0 (Tru64 UNIX edition)
rawVec <- serialize(sample(1:10, 25000000, replace = TRUE), NULL)

test_that("interface for compressing raw vectors", {
  # defaults
  y <- fstcompress(rawVec)
  expect_equal(typeof(y), "raw")

  expect_error(fstcompress(5), "Parameter x is not set to a raw vector")

  expect_error(fstcompress(rawVec, 4), "Parameter compressor should be set")

  expect_error(fstcompress(rawVec, compression = TRUE), "Parameter compression should be a numeric value")
})


test_that("compress return format", {
  y <- fstcompress(rawVec, compressor = "LZ4", compression = 0)
})
