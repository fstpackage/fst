
context("roundtrip-vector")


roundtrip_vector <- function(x) {
  df <- data.frame(x = x, stringsAsFactors = FALSE)
  roundtrip(df)$x
}


roundtrip_vector_dt <- function(x) {
  dt <- data.table(x = x, stringsAsFactors = FALSE)
  roundtrip(dt)$x
}


roundtrip <- function(df) {
  temp <- tempfile()
  fstwriteproxy(df, temp)
  on.exit(unlink(temp))

  fstreadproxy(temp)
}


# Logical
test_that("preserves three logical values", {
  x <- c(FALSE, TRUE, NA)
  expect_identical(roundtrip_vector(x), x)
})


# Integer
test_that("preserves integer values", {
  x <- 1:10
  x[sample(1:10, 3)] <- NA
  expect_identical(roundtrip_vector(x), x)
})


# Double
test_that("preserves special floating point values", {
  x <- c(Inf, -Inf, NaN, NA)
  expect_identical(roundtrip_vector(x), x)
})


test_that("doesn't lose precision", {
  x <- c(1 / 3, sqrt(2), pi)
  expect_identical(roundtrip_vector(x), x)
})


# Character
test_that("preserves character values", {
  x <- c("this is a string", "", NA, "another string")
  expect_identical(roundtrip_vector(x), x)
})


test_that("can have NA on end of string", {
  x <- c("this is a string", NA)
  expect_identical(roundtrip_vector(x), x)
})


# Factor
test_that("preserves simple factor", {
  x <- factor(c("abc", "def"))
  expect_identical(roundtrip_vector(x), x)
})


test_that("preserves NA in factor and levels", {
  x1 <- factor(c("abc", "def", NA))
  x2 <- addNA(x1)

  expect_identical(roundtrip_vector(x1), x1)
  expect_identical(roundtrip_vector(x2), x2)
})


# Date
test_that("preserves dates", {
  x <- as.Date("2010-01-01") + c(0L, 365L, NA)
  mode(x) <- "integer"
  res <- roundtrip_vector(x)
  class(res) <- c("IDate", "Date")  # gains class IDate from data.table
  expect_identical(as.integer(res), as.integer(x))
})


# ITime
test_that("preserves time of day", {
  x <- as.ITime(c(1:10, NA), origin = "1970-01-01")
  res <- roundtrip_vector_dt(x)
  expect_identical(res, x)

  mode(x) <- "double"
  res <- roundtrip_vector_dt(x)
  expect_identical(res, x)
})


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


# nolint start
# Time --------------------------------------------------------------------
#
# test_that("preserves hms", {
#   x <- hms::hms(1:100)
#   expect_identical(roundtrip_vector(x), x)
# })
#
# test_that("converts time to hms", {
#   x1 <- structure(1:100, class = "time")
#   x2 <- roundtrip_vector(x1)
#
#   expect_s3_class(x2, "hms")
# })
#
#
#
#
# test_that("doesn't lose undue precision", {
#   base <- ISOdate(2001, 10, 10)
#   x1 <- base + 1e-6 * (0:3)
#   x2 <- roundtrip_vector(x1)
#
#   expect_identical(x1, x2)
# })
# nolint end
