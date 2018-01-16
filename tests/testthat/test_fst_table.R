
context("fst_table")


# clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


# generate sample data and store
df <- data.frame(X = 1:26, Y = LETTERS, Z = runif(26, 4, 7), stringsAsFactors = FALSE)
test_file <- "testdata/fst_table.fst"
write_fst(df, test_file)
x <- fst(test_file)


test_that("fst_table returns a fst_table class object", {
  expect_equal(class(x) , "fst_table")

  str_object <- unclass(x)
  expect_equal(names(str_object), c("meta", "col_selection", "row_selection", "old_format"))
})


test_that("fst_table has basic data.frame interface", {
  expect_equal(as.data.frame(x), as.data.frame(df))

  expect_equal(as.data.frame(x, row.names = LETTERS), as.data.frame(df, row.names = LETTERS))

  expect_equal(as.list(x), as.list(df))

  expect_equal(x[["Y"]], df[["Y"]])

  expect_equal(x[["S"]], df[["S"]])

  expect_equal(x$X, df$X)

  expect_equal(x$S, df$S)

  expect_equal(nrow(x), nrow(df))

  expect_equal(ncol(x), ncol(df))

  expect_equal(dim(x), dim(df))

  expect_equal(dimnames(x), dimnames(df))

  expect_equal(colnames(x), colnames(df))

  expect_equal(rownames(x), rownames(df))

  expect_equal(names(x), names(df))

  expect_equal(x[[1:2]], df[[1:2]])

  expect_equal(x[[c(2, 4)]], df[[c(2, 4)]])

  expect_equal(x[["G"]], df[["G"]])
})


test_that("fst_table throws errors on incorrect use of interface", {
  expect_error(x[[c("X", 3)]], "subscript out of bounds")

  expect_error(x[[c("X", 3)]], "subscript out of bounds")

  expect_error(x[[as.integer(NULL)]], "Please use a length one integer or")

  expect_error(x[[c(2, 0)]], "Second index out of bounds")

  expect_error(x[[c(3, 100)]], "Second index out of bounds")

  expect_error(x[[5]], "Invalid column index 5")

  expect_error(x[[-3]], "Invalid column index -3")
})
