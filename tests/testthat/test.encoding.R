
context("Encoding")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


testwriteread <- function(x) {
  fstwrite(x, "testdata/encoding.fst")
  y <- fstread("testdata/encoding.fst")
  expect_equal(x, y)
}


test_that("utf8, native and latin1 encodings", {
  native <- data.frame(x = rep("Arende", 5), stringsAsFactors = FALSE)
  utf8 <- data.frame(x = enc2utf8(rep("Ärende", 5)), stringsAsFactors = FALSE)
  latin1 <- data.frame(x = rep("Ärende", 5), stringsAsFactors = FALSE)

  # test encodings
  testwriteread(native)
  testwriteread(latin1)
  testwriteread(utf8)
})


test_that("I can eat glass in various languages", {
  x <- read.csv2("datasets/utf8.csv", encoding = "UTF-8", stringsAsFactors = FALSE)

  testwriteread(x)
})
