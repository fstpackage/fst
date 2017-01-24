
require(testthat)
require(data.table)

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}

context("package interface")

x <- data.table(A = 1:10, B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE))

test_that("From unkeyed data.table to data.table",
{
  write.fst(x, "testdata/nokey.fst")
  
  y <- read.fst("testdata/nokey.fst")
  expect_false(is.data.table(y))

  y <- read.fst("testdata/nokey.fst", as.data.table = TRUE)
  expect_true(is.data.table(y))
})


test_that("From keyed data.table to data.table",
{
  setkey(x, B)
  
  write.fst(x, "testdata/key.fst")

  y <- read.fst("testdata/key.fst")
  expect_false(is.data.table(y))
  
  y <- read.fst("testdata/key.fst", as.data.table = TRUE)
  expect_true(is.data.table(y))
  expect_equal(key(y), "B")
  
  expect_equal(x, y)
})
