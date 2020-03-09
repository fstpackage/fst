
context("keys")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


x <- data.table(A = 1:10, B = 10:1, C = 100:109, D = 20:29, E = 1:10)
setkey(x, A, B, D)


test_that("Fully keyed table", {
  fstwriteproxy(x, "testdata/keys.fst")
  y <- fstreadproxy("testdata/keys.fst", as_data_table = TRUE)
  expect_equal(key(y), key(x))
})


test_that("Missing last key", {
  fstwriteproxy(x, "testdata/keys.fst")
  y <- fstreadproxy("testdata/keys.fst", columns = c("A", "B", "C", "E"), as_data_table = TRUE)
  expect_equal(key(y), c("A", "B"))
})


test_that("Missing middle key", {
  fstwriteproxy(x, "testdata/keys.fst")
  y <- fstreadproxy("testdata/keys.fst", columns = c("A", "C", "D", "E"), as_data_table = TRUE)
  expect_equal(key(y), c("A"))
})


test_that("Missing first key", {
  fstwriteproxy(x, "testdata/keys.fst")
  res <- fst:::fstretrieve("testdata/keys.fst", c("B", "C", "D", "E"), 1L, NULL)
  y <- fstreadproxy("testdata/keys.fst", columns = c("B", "C", "D", "E"), as_data_table = TRUE)
  expect_null(key(y))
})
