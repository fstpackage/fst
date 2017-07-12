
context("encoding")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


testwriteread <- function(x, uniform.encoding = TRUE) {
  fstwriteproxy(x, "testdata/encoding.fst", uniform.encoding)
  y <- fstreadproxy("testdata/encoding.fst")
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


test_that("Mixed encodings", {
  x <- data.frame(Mixed = c("native", "Ärende", enc2utf8("Ärende")), stringsAsFactors = FALSE)

  fstwriteproxy(x, "testdata/mixed.fst")
  y <- fstreadproxy("testdata/mixed.fst")

  expect_failure(expect_equal(x, y))  # encoding is assumed to be latin1

  expect_error(fstwriteproxy(x, "testdata/mixed.fst", uniform.encoding = FALSE),
    "Character vectors with mixed encodings are currently not supported")

  Encoding(x$Mixed[3]) <- "latin1"  # only latin1 and native encoding now
  expect_error(fstwriteproxy(x, "testdata/mixed.fst", uniform.encoding = FALSE),
    "Character vectors with mixed encodings are currently not supported")

  x <- data.frame(Uniform = LETTERS, stringsAsFactors = FALSE)
  testwriteread(x, FALSE)
})
