
context("compression")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("interface for compressing general R object", {
  x <- list(1, "A", 3.14, factor(LETTERS[2:5]))

  # no compression
  y <- fst:::fstcompress(x, compressor = "LZ4", compression = 100)
  expect_equal(typeof(y), "raw")
})
