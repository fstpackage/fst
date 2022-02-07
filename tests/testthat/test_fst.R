
context("subsetting and compression")


# some helper functions
source("helper_fstwrite.R")


# Clean testdata directory
if (!file.exists("FactorStore")) {
  dir.create("FactorStore")
} else {
  file.remove(list.files("FactorStore", full.names = TRUE))
}

# Create a pool of strings
nroflevels <- 8

char_vec <- function(nr_of_rows) {
  sapply(1:nr_of_rows, function(x) {
    paste(sample(LETTERS, sample(1:4, 1)), collapse = "")
  }
  ) }

char_veclong <- function(nr_of_rows) {
  sapply(1:nr_of_rows,
    function(x) {
      paste(sample(LETTERS, sample(20:25, 1)), collapse = "")
    }
    ) }

date_vec <- function(nr_of_rows) {
  date_vec <- sample(1:nr_of_rows, replace = TRUE)
  class(date_vec) <- c("IDate", "Date")
  date_vec
}

difftime_vec <- function(nr_of_rows, mode = "double") {
  vec <- (Sys.time() + 1:nr_of_rows) - Sys.time()
  mode(vec) <- mode
  vec
}

# Sample data
nr_of_rows <- 10000L
char_na <- char_vec(nr_of_rows)
char_na[sample(1:nr_of_rows, 10)] <- NA
datatable <- data.frame(
  Xint = 1:nr_of_rows,
  Ylog = sample(c(TRUE, FALSE, NA), nr_of_rows, replace = TRUE),
  Zdoub = rnorm(nr_of_rows),
  Qchar = char_vec(nr_of_rows),
  WFact = factor(sample(char_vec(nroflevels), nr_of_rows, replace = TRUE)),
  Ordered = ordered(sample(char_vec(nroflevels), nr_of_rows, replace = TRUE)),
  char_na = char_na,
  CharLong = char_veclong(nr_of_rows),
  Date = date_vec(nr_of_rows),
  DateDouble = as.Date("2015-01-01") + 1:nr_of_rows,
  Raw = as.raw(sample(0:255, nr_of_rows, replace = TRUE)),
  Difftime = difftime_vec(nr_of_rows),
  DiffTime_int = difftime_vec(nr_of_rows, "integer"),
  stringsAsFactors = FALSE)


# A write / read cylce for a range of columns and rows
test_write_read <- function(col, from = 1L, to = nr_of_rows, sel_columns = NULL, compress = 0L,
  tot_length = nr_of_rows) {
  dt <- datatable[1:tot_length, col, drop = FALSE]

  fstwriteproxy(dt, "FactorStore/data1.fst", compress)  # use compression

  # Read full dataset
  to <- min(to, nr_of_rows)
  data <- fstreadproxy("FactorStore/data1.fst", columns = sel_columns, from = from, to = to)

  if (is.null(sel_columns)) {
    sub_dt <- dt[from:to, , drop = FALSE]
  } else {
    sub_dt <- dt[from:to, sel_columns, drop = FALSE]
  }

  row.names(sub_dt) <- NULL

  uneq <- sub_dt[, 1] != data[, 1]
  difftable <- sub_dt
  difftable[, "Row"] <- seq_len(nrow(difftable))
  difftable[, "Other"] <- data[, 1]

  message <- paste(
    "args: col:", col, "| from:", from, "| to:", to, "| setColumns:", sel_columns,
    "| compress:", compress, "| tot_length", tot_length, " cols sub_dt:", ncol(sub_dt), ", rows sub_dt:",
      nrow(sub_dt), "cols data:", ncol(data), ", rows data:", nrow(data),
    "head sub_dt:", paste(sub_dt[1:10, 1], collapse = ","),
    "head data:", paste(data[1:10, 1], collapse = ","),
    "unequals:", sum(uneq),
    "uneq rows sub_dt1", paste(difftable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 1], collapse = ","),
    "uneq rows sub_dt2", paste(difftable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 2], collapse = ","),
    "uneq rows sub_dt3", paste(difftable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 3], collapse = ","))

  expect_equal(sub_dt, data, info = message)
}

col_names <- colnames(datatable)


test_that("Single uncompressed vectors", {
  sapply(col_names,
    function(x) {
      test_write_read(x)
    })
})


test_that("Small uncompressed vectors", {
  sapply(col_names, function(x) {
    test_write_read(x, to = 30L, tot_length = 30L)
  })
})


test_that("Single weakly compressed vectors", {
  sapply(col_names,
    function(x) {
      test_write_read(x, compress = 30L)
    })
})


test_that("Single small weakly compressed vectors", {
  sapply(col_names,
    function(x) {
      test_write_read(x, to = 30L, tot_length = 30L, compress = 30L)
    })
})


test_that("Single medium sized weakly compressed vectors", {
  sapply(col_names,
    function(x) {
      test_write_read(x, to = 8193L, tot_length = 8193L, compress = 30L)
    })
})


test_that("Single moderate compressed vectors", {
  sapply(col_names,
    function(x) {
      test_write_read(x, compress = 60L)
    })
})


test_that("Single small moderate compressed vectors", {
  sapply(col_names,
    function(x) {
      test_write_read(x, to = 30L, tot_length = 30L, compress = 60L)
  })
})


# Various boundary conditions

blocktests <- function(col, block_start, block_end, compression) {
  last_row <- min(block_end * block_size + block_size - 1L, nr_of_rows)
  test_write_read(col, 1L + block_start * block_size,      last_row,       NULL, compression)  # full
  test_write_read(col, 1L + block_start * block_size + 4L, last_row,       NULL, compression)  # offset
  test_write_read(col, 1L + block_start * block_size,      last_row - 10L, NULL, compression)  # remainder
}


blocktestsingletype <- function(type) {
  # Single first block
  blocktests(type, 0, 0, 0L)  # uncompressed
  blocktests(type, 0, 0, 40L)  # algorithm 1
  blocktests(type, 0, 0, 80L)  # algorithm 2

  # Single middle block
  blocktests(type, 1, 1, 0L)  # uncompressed
  blocktests(type, 1, 1, 40L)  # algorithm 1
  blocktests(type, 1, 1, 80L)  # algorithm 2

  last_block <- as.integer((nr_of_rows - 1) / block_size)  ## nolint

  # Single last block
  blocktests(type, last_block, last_block, 0L)  # uncompressed
  blocktests(type, last_block, last_block, 40L)  # algorithm 1
  blocktests(type, last_block, last_block, 80L)  # algorithm 2

  # Multiple blocks
  blocktests(type, 0, 1, 0L)  # uncompressed
  blocktests(type, last_block - 1, last_block, 0L)  # uncompressed
  blocktests(type, 0, last_block, 0L)  # uncompressed

  blocktests(type, 0, 1, 40L)  # algorithm 1
  blocktests(type, last_block - 1, last_block, 40L)  # algorithm 1
  blocktests(type, 0, last_block, 40L)  # algorithm 1

  blocktests(type, 0, 1, 80L)  # algorithm 2
  blocktests(type, last_block - 1, last_block, 80L)  # algorithm 2
  blocktests(type, 0, last_block, 80L)  # algorithm 2
}


block_size <- 4096

# Test blocks
test_that("Integer column block tests", {
  blocktestsingletype("Xint")
})


test_that("Logical column block tests", {
  blocktestsingletype("Ylog")
})


test_that("Date column block tests", {
  blocktestsingletype("Date")
})

block_size <- 2048


# Test blocks
test_that("Real column block tests", {
  blocktestsingletype("Zdoub")
})


block_size <- 2047

test_that("Character column block tests", {
  blocktestsingletype("Qchar")
})


test_that("Factor column block tests", {
  blocktestsingletype("WFact")
})


test_that("Character column block tests with NA's", {
  blocktestsingletype("char_na")
})


test_that("Mixed columns are stored correctly", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"))
})


test_that("From and to row can be set", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"), from = 10)
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"), to = 8)
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"), from = 4, to = 13)
})


test_that("Select columns", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"),
    sel_columns = "Zdoub")
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"),
    sel_columns = c("Ylog", "WFact"))
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"),
    sel_columns = c("WFact", "Ylog"))
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "char_na", "Date"),
    from = 7, to = 13, sel_columns = c("Ylog", "Qchar"))
})


test_that("Select unknown column", {
  expect_error(data <- fstreadproxy("FactorStore/data1.fds", columns = "bla"))
  expect_error(data <- fstreadproxy("FactorStore/data1.fds", columns = c("WFact", "bla")))
})


test_that("Select out of range row number", {
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact"), from = 4, to = 7000)
  test_write_read(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact"), from = 4, to = NULL)
  expect_error(fstreadproxy("FactorStore/data1.fst", from = 12000, to = NULL),
    "Row selection is out of range")
  expect_error(fstreadproxy("FactorStore/data1.fst", from = 0, to = NULL), "Parameter 'from' should have")
})
