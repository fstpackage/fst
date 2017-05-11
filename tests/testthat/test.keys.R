
context("keys")

source("helper.fstwrite.R")

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


x <- data.table(A = 1:10, B = 10:1, C = 100:109, D = 20:29, E = 1:10)
setkey(x, A, B, D)


test_that("Fully keyed table",
{
  fstwrite(x, "testdata/keys.fst")
  y <- fstread("testdata/keys.fst", as.data.table = TRUE)
  expect_equal(key(y), key(x))
})


test_that("Missing last key",
{
  fstwrite(x, "testdata/keys.fst")
  y <- fstread("testdata/keys.fst", columns = c("A", "B", "C", "E"), as.data.table = TRUE)
  expect_equal(key(y), c("A", "B"))
})


test_that("Missing middle key",
{
  fstwrite(x, "testdata/keys.fst")
  y <- fstread("testdata/keys.fst", columns = c("A", "C", "D", "E"), as.data.table = TRUE)
  expect_equal(key(y), c("A"))
})


test_that("Missing first key",
{
  fstwrite(x, "testdata/keys.fst")
  res <- fst:::fstRetrieve("testdata/keys.fst", c("B", "C", "D", "E"), 1L, NULL)
  y <- fstread("testdata/keys.fst", columns = c("B", "C", "D", "E"), as.data.table = TRUE)
  expect_null(key(y))
})
