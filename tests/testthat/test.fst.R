
context("subsetting and compression")

library(bit64)


# Clean testdata directory
if (!file.exists("FactorStore")) {
  dir.create("FactorStore")
} else {
  file.remove(list.files("FactorStore", full.names = TRUE))
}

# Create a pool of strings
nroflevels <- 8

char_vec <- function(nrofrows) {
  sapply(1:nrofrows, function(x) {
    paste(sample(LETTERS, sample(1:4)), collapse = "")
  }
  ) }

char_veclong <- function(nrofrows) {
  sapply(1:nrofrows,
    function(x) {
      paste(sample(LETTERS, sample(20:25)), collapse = "")
    }
    ) }

date_vec <- function(nrofrows) {
  date_vec <- sample(1:nrofrows, replace = TRUE)
  class(date_vec) <- "Date"
  date_vec
}

# Sample data
nrofrows <- 10000L
char_na <- char_vec(nrofrows)
char_na[sample(1:nrofrows, 10)] <- NA
datatable <- data.frame(
  Xint = 1:nrofrows,
  Ylog = sample(c(TRUE, FALSE, NA), nrofrows, replace = TRUE),
  Zdoub = rnorm(nrofrows),
  Qchar = char_vec(nrofrows), WFact = factor(sample(char_vec(nroflevels), nrofrows, replace = TRUE)),
  char_na = char_na,
  CharLong = char_veclong(nrofrows),
  Date = date_vec(nrofrows),
  Integer64 = as.integer64(sample(c(2345612345679, 10, 8714567890), nrofrows, replace = TRUE)),
  stringsAsFactors = FALSE)


test_write_read <- function(col, from = 1L, to = nrofrows, sel_columns = NULL, compress = 0L, tot_length = nrofrows) {
  dt <- datatable[1:tot_length, col, drop = FALSE]

  fstwriteproxy(dt, "FactorStore/data1.fst", compress)  # use compression

  # Read full dataset
  to <- min(to, nrofrows)
  data <- fstreadproxy("FactorStore/data1.fst", columns = sel_columns, from = from, to = to)

  if (is.null(sel_columns)) {
    sub_dt <- dt[from:to, , drop = FALSE]
  } else {
    sub_dt <- dt[from:to, sel_columns, drop = FALSE]
  }

  row.names(sub_dt) <- NULL

  uneq <- sub_dt[, 1] != data[, 1]
  diffTable <- sub_dt
  diffTable$Row <- 1:nrow(diffTable)
  diffTable$Other <- data[, 1]

  message <- paste(
    "args: col:", col, "| from:", from, "| to:", to, "| setColumns:", sel_columns,
    "| compress:", compress, "| tot_length", tot_length, " cols sub_dt:", ncol(sub_dt), ", rows sub_dt:", nrow(sub_dt),
    "cols data:", ncol(data), ", rows data:", nrow(data),
    "head sub_dt:", paste(sub_dt[1:10, 1], collapse = ","),
    "head data:", paste(data[1:10, 1], collapse = ","),
    "unequals:", sum(uneq),
    "uneq rows sub_dt1", paste(diffTable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 1], collapse = ","),
    "uneq rows sub_dt2", paste(diffTable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 2], collapse = ","),
    "uneq rows sub_dt3", paste(diffTable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 3], collapse = ","))

  expect_equal(sub_dt, data, info = message)
}

colNames <- colnames(datatable)

test_that("Single uncompressed vectors",
{
  sapply(colNames, function(x){test_write_read(x)})
})


test_that("Small uncompressed vectors",
{
  sapply(colNames, function(x){test_write_read(x, to = 30L, tot_length = 30L)})
})


test_that("Single weakly compressed vectors",
{
  sapply(colNames, function(x){test_write_read(x, compress = 30L)})
})


test_that("Single small weakly compressed vectors",
{
  sapply(colNames, function(x){test_write_read(x, to = 30L, tot_length = 30L, compress = 30L)})
})


test_that("Single medium sized weakly compressed vectors",
{
  sapply(colNames, function(x){test_write_read(x, to = 8193L, tot_length = 8193L, compress = 30L)})
})


test_that("Single moderate compressed vectors",
{
  # sapply(colNames, function(x){test_write_read(x, compress = 60L)})
})


test_that("Single small moderate compressed vectors",
{
  # sapply(colNames, function(x){test_write_read(x, to = 30L, tot_length = 30L, compress = 60L)})
})


# Various boundary conditions

BlockTests <- function(col, blockStart, blockEnd, compression)
{
  lastRow = min(blockEnd * blockSize + blockSize - 1L, nrofrows)
  test_write_read(col, 1L + blockStart * blockSize,      lastRow,       NULL, compression)  # full
  test_write_read(col, 1L + blockStart * blockSize + 4L, lastRow,       NULL, compression)  # offset
  test_write_read(col, 1L + blockStart * blockSize,      lastRow - 10L, NULL, compression)  # remainder
}


BlockTestSingleType <- function(type)
{
  # Single first block
  BlockTests(type, 0, 0, 0L )  # uncompressed
  BlockTests(type, 0, 0, 40L)  # algorithm 1
  BlockTests(type, 0, 0, 80L)  # algorithm 2

  # Single middle block
  BlockTests(type, 1, 1, 0L )  # uncompressed
  BlockTests(type, 1, 1, 40L)  # algorithm 1
  BlockTests(type, 1, 1, 80L)  # algorithm 2

  lastBlock = as.integer((nrofrows - 1) / blockSize)

  # Single last block
  BlockTests(type, lastBlock, lastBlock, 0L )  # uncompressed
  BlockTests(type, lastBlock, lastBlock, 40L)  # algorithm 1
  BlockTests(type, lastBlock, lastBlock, 80L)  # algorithm 2

  # Multiple blocks
  BlockTests(type, 0, 1, 0L)  # uncompressed
  BlockTests(type, lastBlock - 1, lastBlock, 0L)  # uncompressed
  BlockTests(type, 0, lastBlock, 0L)  # uncompressed

  BlockTests(type, 0, 1, 40L)  # algorithm 1
  BlockTests(type, lastBlock - 1, lastBlock, 40L)  # algorithm 1
  BlockTests(type, 0, lastBlock, 40L)  # algorithm 1

  # BlockTests(type, 0, 1, 80L)  # algorithm 2
  # BlockTests(type, lastBlock - 1, lastBlock, 80L)  # algorithm 2
  # BlockTests(type, 0, lastBlock, 80L)  # algorithm 2
}


blockSize = 4096

# Test blocks
test_that("Integer column block tests", {
  BlockTestSingleType("Xint")
})


test_that("Logical column block tests", {
  BlockTestSingleType("Ylog")
})


test_that("Date column block tests", {
  BlockTestSingleType("Date")
})

blockSize = 2048


# Test blocks
test_that("Real column block tests", {
  BlockTestSingleType("Zdoub")
})


test_that("Integer64 column block tests", {
  BlockTestSingleType("Integer64")
})

blockSize = 2047

test_that("Character column block tests", {
  BlockTestSingleType("Qchar")
})


test_that("Factor column block tests", {
  BlockTestSingleType("WFact")
})


test_that("Character column block tests with NA's", {
  BlockTestSingleType("char_na")
})


test_that("Mixed columns are stored correctly", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"))
})


test_that("From and to row can be set", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), from = 10)
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), to = 8)
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), from = 4, to = 13)
})


test_that("Select columns", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), sel_columns = "Zdoub")
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), sel_columns = c("Ylog", "WFact"))
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), sel_columns = c("WFact", "Ylog"))
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date", "Integer64"), from = 7, to = 13, sel_columns = c("Ylog", "Qchar"))
})


test_that("Select unknown column", {
  expect_error(data <- fstreadproxy("FactorStore/data1.fds", columns = "bla"))
  expect_error(data <- fstreadproxy("FactorStore/data1.fds", columns = c("WFact", "bla")))
})


test_that("Select out of range row number", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact"), from = 4, to = 7000)
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact"), from = 4, to = NULL)
  expect_error(fstreadproxy("FactorStore/data1.fst", from = 12000, to = NULL), "Row selection is out of range")
  expect_error(fstreadproxy("FactorStore/data1.fst", from = 0, to = NULL), "Parameter 'from' should have")
})
