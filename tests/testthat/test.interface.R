
context("package interface")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


x <- data.table(A = 1:10, B = sample(c(TRUE, FALSE, NA), 10, replace = TRUE))
xkey <- readRDS("keyedTable.rds")  # import keyed table to avoid memory leaks in data.table (LeakSanitizer)

test_that("From unkeyed data.table to data.table", {
  fstwriteproxy(x, "testdata/nokey.fst")

  y <- fstreadproxy("testdata/nokey.fst")
  expect_false(is.data.table(y))

  y <- fstreadproxy("testdata/nokey.fst", as.data.table = TRUE)
  expect_null(key(y))
  expect_true(is.data.table(y))

  z <- fstmetaproxy("testdata/nokey.fst")  # expect no error
})


test_that("From keyed data.table to data.table", {
  fstwriteproxy(xkey, "testdata/key.fst")

  y <- fstreadproxy("testdata/key.fst")
  expect_false(is.data.table(y))

  y <- fstreadproxy("testdata/key.fst", as.data.table = TRUE)
  expect_true(is.data.table(y))
  expect_equal(key(y), "B")
  expect_equal(xkey, y)

  z <- fstmetaproxy("testdata/key.fst")  # expect no error
})
