
context("encoding")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


testwriteread <- function(x, encoding, uniform.encoding = TRUE) {
  fstwriteproxy(x, "testdata/encoding.fst", uniform.encoding)
  y <- fstreadproxy("testdata/encoding.fst")

  for (col in 1:ncol(x)) {
    if (typeof(x[[col]]) == "character") {
      expect_equal(Encoding(y[[col]]), rep(encoding[col], nrow(x)))
    }
  }

  expect_equal(x, y)
}


test_that("utf8, native and latin1 encodings", {
  native <- data.frame(x = rep("Arende", 5), stringsAsFactors = FALSE)
  latin1 <- data.frame(x = rep("Ärende", 5), stringsAsFactors = FALSE)
  Encoding(latin1$x) <- "latin1"
  utf8 <- data.frame(x = enc2utf8(rep("Ärende", 5)), stringsAsFactors = FALSE)

  # test encodings
  testwriteread(native, c("unknown"))
  testwriteread(latin1, c("latin1"))
  testwriteread(utf8, "UTF-8")
})


test_that("I can eat glass in various languages", {
  x <- read.csv2("datasets/utf8.csv", encoding = "UTF-8", stringsAsFactors = FALSE)

  testwriteread(x, c("unknown", "UTF-8"))
})


test_that("Mixed encodings", {
  x <- data.frame(Mixed = c("Ärende", enc2native("native"), enc2utf8("Ärende")), stringsAsFactors = FALSE)
  Encoding(x$Mixed[1]) <- "latin1"  # be sure

  fstwriteproxy(x, "testdata/mixed.fst")
  y <- fstreadproxy("testdata/mixed.fst")

  expect_equal(Encoding(y$Mixed), c("latin1", "unknown", "latin1"))  # recoded to latin1

  expect_failure(expect_equal(x, y))  # mix of unknown, latin1 and UTF-8

  expect_error(fstwriteproxy(x, "testdata/mixed.fst", uniform.encoding = FALSE),
    "Character vectors with mixed encodings are currently not supported")

  Encoding(x$Mixed[1]) <- "UTF-8"  # mix of unknown and UTF-8
  expect_error(fstwriteproxy(x, "testdata/mixed.fst", uniform.encoding = FALSE),
    "Character vectors with mixed encodings are currently not supported")

  fstwriteproxy(x, "testdata/mixed.fst")
  y <- fstreadproxy("testdata/mixed.fst")

  expect_equal(Encoding(y$Mixed), c("UTF-8", "unknown", "UTF-8"))  # recoded to UTF-8
})


test_that("Column name encoding", {
  x <- read.csv2("datasets/utf8.csv", encoding = "UTF-8", stringsAsFactors = FALSE)[2, 2]

  df <- data.frame(X = 1:10)
  colnames(df) <- x

  fstwriteproxy(df, "testdata/enc_cols.fst")
  y <- fstreadproxy("testdata/enc_cols.fst")

  expect_equal(Encoding(colnames(y)), "UTF-8")
})
