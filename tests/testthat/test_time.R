
context("time")


roundtrip_vector <- function(x) {
  df <- data.frame(x = x, stringsAsFactors = FALSE)
  roundtrip(df)$x
}


roundtrip <- function(df) {
  temp <- tempfile()
  fstwriteproxy(df, temp)
  on.exit(unlink(temp))

  fstreadproxy(temp)
}


# POSIXct
test_that("preserves times", {
  x1 <- ISOdate(2001, 10, 10, tz = "US/Pacific") + c(0, NA)
  x2 <- roundtrip_vector(x1)

  expect_identical(attr(x1, "tzone"), attr(x2, "tzone"))
  expect_identical(attr(x1, "class"), attr(x1, "class"))
  expect_identical(unclass(x1), unclass(x2))

  mode(x1) <- "integer"
  x2 <- roundtrip_vector(x1)

  expect_identical(attr(x1, "tzone"), attr(x2, "tzone"))
  expect_identical(attr(x1, "class"), attr(x1, "class"))
  expect_identical(unclass(x1), unclass(x2))
})


test_that("throws error on POSIXlt", {
  df <- data.frame(x = Sys.time())
  df$x <- as.POSIXlt(df$x)

  expect_error(roundtrip(df), "Unknown type found in column")
})


test_that("empty timezone attribute", {
  x1 <- Sys.time()

  # set to empty time zone
  attributes(x1)$tzone <- ""

  x2 <- roundtrip_vector(x1)

  expect_identical(attr(x1, "tzone"), attr(x2, "tzone"))
  expect_identical(x1, x2)

  mode(x1) <- "integer"

  x2 <- roundtrip_vector(x1)

  expect_identical(attr(x1, "tzone"), attr(x2, "tzone"))
  expect_identical(x1, x2)
})


test_that("doesn't lose undue precision", {
  base <- ISOdate(2001, 10, 10)
  x1 <- base + 1e-6 * (0:3)
  x2 <- roundtrip_vector(x1)

  expect_identical(x1, x2)
})


test_that("NULL tzone attribute is retained", {
  x1 <- Sys.time()
  attributes(x1)$tzone <- NULL  # no tzone set

  expect_equal(typeof(x1), "double")

  # write / read cycle
  x2 <- roundtrip_vector(x1)

  expect_equal(typeof(x2), "double")
  expect_null(attributes(x2)$tzone)

  mode(x1) <- "integer"
  expect_equal(typeof(x1), "integer")

  # write / read cycle
  x2 <- roundtrip_vector(x1)
  expect_equal(typeof(x2), "integer")
  expect_null(attributes(x2)$tzone)
})
