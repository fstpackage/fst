
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

  expect_equal(x[2], df[2])

  expect_equal(x[2.6], df[2.6])

  expect_equal(x[TRUE], df[TRUE])

  expect_equal(x[c(1.1, 2)], df[c(1.1, 2)])

  expect_equal(x[c(TRUE, FALSE)], df[c(TRUE, FALSE)])

  expect_equal(x[c(TRUE, FALSE, TRUE)], df[c(TRUE, FALSE, TRUE)])

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


test_that("fst_table [ generic", {

  # '[' generic with 2 arguments

  expect_equal(x[], df[])

  expect_equal(x[2], df[2])

  expect_equal(x[i = 2], df[2])

  expect_equal(x[j = 2], df[2])

  expect_equal(x[drop = FALSE], df[,])

  # '[' generic with 3 arguments

  expect_equal(x[,], df[,])

  expect_equal(x[,2:3], df[, 2:3])

  expect_equal(x[j = 2:3, drop = FALSE], df[, 2:3])

  expect_equal(x[i = 2:1, drop = FALSE], df[, 2:1])

  expect_equal(as.list(x[2,]), as.list(df[2,]))

  expect_equal(as.list(x[2:10,]), as.list(df[2:10,]))

  expect_equal(x[2, drop = FALSE], df[2])

  expect_equal(as.list(x[2, 1:3]), as.list(df[2, 1:3]))

  # '[' generic with 4 arguments

  expect_equal(x[,, drop = FALSE], df[])

  expect_equal(x[,,], df[])

  expect_equal(x[j = 2, drop = FALSE], df[2])
})


test_that("fst_table throws errors on incorrect use of interface", {
  expect_error(x[[c("X", 3)]], "Subscript out of bounds")

  expect_error(x[[c("X", 3)]], "Subscript out of bounds")

  expect_error(x[4], "Subscript out of bounds")

  expect_error(x[5.3], "Subscript out of bounds")

  expect_error(x[c(1, NA, 2)], "Subscript out of bounds")

  expect_error(x[c(1.1, NA, 2)], "Subscript out of bounds")

  expect_error(x[c(TRUE, NA)], "Subscript out of bounds")

  expect_error(x[[as.integer(NULL)]], "Please use a length one integer or")

  expect_error(x[[c(2, 0)]], "Second index out of bounds")

  expect_error(x[[c(3, 100)]], "Second index out of bounds")

  expect_error(x[[5]], "Invalid column index 5")

  expect_error(x[[-3]], "Invalid column index -3")
})


test_that("fst_table has correct printing for small single column table", {
  df <- data.frame(X = 1)
  write_fst(df, test_file)
  x <- fst(test_file)

  res <- capture_output(print(x))
  res <- crayon::strip_style(res)

  expect_equal(res, paste(
    "<fst file>",
    "1 rows, 1 columns (fst_table.fst)\n",
    "         X",
    "  <double>",
    "1        1",
    sep = "\n"))
})


test_that("fst_table has correct printing for big single column table", {
  df <- data.frame(X = 1:100)
  write_fst(df, test_file)
  x <- fst(test_file)

  res <- capture_output(print(x))
  res <- crayon::strip_style(res)

  expect_equal(res, paste(
    "<fst file>",
    "100 rows, 1 columns (fst_table.fst)\n",
    "            X",
    "    <integer>",
    "1           1",
    "2           2",
    "3           3",
    "4           4",
    "5           5",
    "--         --",
    "96         96",
    "97         97",
    "98         98",
    "99         99",
    "100       100",
    sep = "\n"))
})


test_that("fst_table has correct printing for big multi column table", {
  df <- data.frame(X = 1:104, Y = c(LETTERS, LETTERS, LETTERS, LETTERS))
  write_fst(df, test_file)
  x <- fst(test_file)

  res <- capture_output(print(x))
  res <- crayon::strip_style(res)

  expect_equal(res, paste(
    "<fst file>",
    "104 rows, 2 columns (fst_table.fst)\n",
    "            X        Y",
    "    <integer> <factor>",
    "1           1        A",
    "2           2        B",
    "3           3        C",
    "4           4        D",
    "5           5        E",
    "--         --       --",
    "100       100        V",
    "101       101        W",
    "102       102        X",
    "103       103        Y",
    "104       104        Z",
    sep = "\n"))
})


test_that("fst_table has correct printing for small multi column table", {
  df <- data.frame(X = 1:9, Y = LETTERS[10:18])
  write_fst(df, test_file)
  x <- fst(test_file)

  res <- capture_output(print(x))
  res <- crayon::strip_style(res)

  expect_equal(res, paste(
    "<fst file>",
    "9 rows, 2 columns (fst_table.fst)\n",
    "          X        Y",
    "  <integer> <factor>",
    "1         1        J",
    "2         2        K",
    "3         3        L",
    "4         4        M",
    "5         5        N",
    "6         6        O",
    "7         7        P",
    "8         8        Q",
    "9         9        R",
    sep = "\n"))
})
