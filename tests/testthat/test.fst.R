
context("factor column")


# Clean testdata directory
if (!file.exists("FactorStore"))
{
  dir.create("FactorStore")
} else
{
  file.remove(list.files("FactorStore", full.names = TRUE))
}

# Create a pool of strings
nrOfLevels <- 8
CharVec <- function(nrOfRows) { sapply(1:nrOfRows, function(x) { paste(sample(LETTERS, sample(1:4)), collapse="") }) }
CharVecLong <- function(nrOfRows) { sapply(1:nrOfRows, function(x) { paste(sample(LETTERS, sample(20:25)), collapse="") }) }

# Sample data
nrOfRows <- 10000L
charNA <- CharVec(nrOfRows)
charNA[sample(1:nrOfRows, 10)] <- NA
dataTable <- data.frame(Xint=1:nrOfRows, Ylog=sample(c(TRUE, FALSE, NA), nrOfRows, replace=TRUE),
  Zdoub=rnorm(nrOfRows), Qchar=CharVec(nrOfRows), WFact=factor(sample(CharVec(nrOfLevels), nrOfRows, replace = TRUE)),
  CharNA = charNA,
  CharLong = CharVecLong(nrOfRows),
  stringsAsFactors = FALSE)


# col = "Qchar"
# from = 1L
# compress = 60L
# to = totLength = nrOfRows
# selColumns = NULL
#
# require(testthat)
# require(microbenchmark)
#
# Bench <- function(dataSet)
# {
#   print(microbenchmark(
#     {
#       write.fst(dataSet, "FactorStore/data0.fst")  # no compression
#     }, times = 1))
#
#   print(microbenchmark(
#     {
#       write.fst(dataSet, "FactorStore/data1.fst", 1L)  # use compression
#     }, times = 1))
#
#   print(microbenchmark(
#     {
#       write.fst(dataSet, "FactorStore/data30.fst", 30L)  # use compression
#     }, times = 1))
#
#   print(microbenchmark(
#     {
#       write.fst(dataSet, "FactorStore/data70.fst", 70L)  # use compression
#     }, times = 1))
#
#   print(microbenchmark(
#     {
#       write.fst(dataSet, "FactorStore/data100.fst", 100L)  # use compression
#     }, times = 1))
# }
#
# dt <- data.frame(Integer = 1:100000000)
# object.size(dt) * 1e-9
# Bench(dt)
#
# dt <- data.frame(Logical = sample(c(TRUE, FALSE, NA), 100000000, replace = TRUE))
# object.size(dt) * 1e-9
# Bench(dt)
#
#
# dt <- data.frame(Double = runif(100000000, 0.0, 1000.0))
# object.size(dt) * 1e-9
# Bench(dt)
#
# rm(dt)

# read.fst("FactorStore/data1.fst")
#
# #
# dt <- dataTable[1:totLength, col, drop = FALSE]
# fstwrite(dt, "FactorStore/data1.fst", compress)  # use compression
#
# fstversion1::write.fst(dt, "FactorStore/data1.fst", compress)  # use compression
#
# fstversion1::fstread("FactorStore/data1.fst")
# fst::fstread("FactorStore/data1.fst")
#
#
# fst:::fstMeta("FactorStore/data1.fst")
# fstread("FactorStore/data1.fst")

# col = c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA")

# fst:::fstRead("FactorStore/data1.fst", selColumns, 1L, to)

col = "Xint"
from = 1L
to = 30L
selColumns = NULL
totLength = 30L
compress = 30L

TestWriteRead <- function(col, from = 1L, to = nrOfRows, selColumns = NULL, compress = 0L, totLength = nrOfRows)
{
  dt <- dataTable[1:totLength, col, drop = FALSE]

  fstwrite(dt, "FactorStore/data1.fst", compress)  # use compression

  # Read full dataset
  to = min(to, nrOfRows)
  data <- fstread("FactorStore/data1.fst", columns = selColumns, from = from, to = to)

  if (is.null(selColumns))
  {
    subDT <- dt[from:to, , drop = FALSE]
  } else
  {
    subDT <- dt[from:to, selColumns, drop = FALSE]
  }

  row.names(subDT) <- NULL

  uneq <- subDT[, 1] != data[, 1]
  diffTable <- subDT
  diffTable$Row <- 1:nrow(diffTable)
  diffTable$Other <- data[, 1]

  message <- paste(
    "args: col:", col, "| from:", from, "| to:", to, "| setColumns:", selColumns,
    "| compress:", compress, "| totLength", totLength, " cols subDT:", ncol(subDT), ", rows subDT:", nrow(subDT),
    "cols data:", ncol(data), ", rows data:", nrow(data),
    "head subDT:", paste(subDT[1:10, 1], collapse = ","),
    "head data:", paste(data[1:10, 1], collapse = ","),
    "unequals:", sum(uneq),
    "uneq rows subDT1", paste(diffTable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 1], collapse = ","),
    "uneq rows subDT2", paste(diffTable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 2], collapse = ","),
    "uneq rows subDT3", paste(diffTable[uneq, ][1:min(25, sum(uneq, na.rm = TRUE)), 3], collapse = ","))

  expect_equal(subDT, data, info = message)
}

colNames <- colnames(dataTable)

test_that("Single uncompressed vectors",
{
  sapply(colNames, function(x){TestWriteRead(x)})
})


test_that("Small uncompressed vectors",
{
  sapply(colNames, function(x){TestWriteRead(x, to = 30L, totLength = 30L)})
})


#
# dt <- dataTable[, "Qchar", drop = FALSE]
# fstwrite(dt, "FactorStore/char_uncomp_10000.fst")
# fstwrite(dt, "FactorStore/data1.fst", compress)  # use compression
#
# read.fst("E:\\Repositories\\BitBucket\\fst\\vs\\testData\\char_comp_10000.fst")


test_that("Single weakly compressed vectors",
{
  sapply(colNames, function(x){TestWriteRead(x, compress = 30L)})
})


test_that("Single small weakly compressed vectors",
{
  sapply(colNames, function(x){TestWriteRead(x, to = 30L, totLength = 30L, compress = 30L)})
})


test_that("Single medium sized weakly compressed vectors",
{
  sapply(colNames, function(x){TestWriteRead(x, to = 8193L, totLength = 8193L, compress = 30L)})
})


test_that("Single moderate compressed vectors",
{
  # sapply(colNames, function(x){TestWriteRead(x, compress = 60L)})
})


test_that("Single small moderate compressed vectors",
{
  # sapply(colNames, function(x){TestWriteRead(x, to = 30L, totLength = 30L, compress = 60L)})
})


# Various boundary conditions

BlockTests <- function(col, blockStart, blockEnd, compression)
{
  lastRow = min(blockEnd * blockSize + blockSize - 1L, nrOfRows)
  TestWriteRead(col, 1L + blockStart * blockSize,      lastRow,       NULL, compression)  # full
  TestWriteRead(col, 1L + blockStart * blockSize + 4L, lastRow,       NULL, compression)  # offset
  TestWriteRead(col, 1L + blockStart * blockSize,      lastRow - 10L, NULL, compression)  # remainder
}


BlockTestSingleType <- function(type)
{
  # Single first block
  BlockTests(type, 0, 0, 0L )  # uncompressed
  BlockTests(type, 0, 0, 40L)  # algorithm 1
  # BlockTests(type, 0, 0, 80L)  # algorithm 2

  # Single middle block
  BlockTests(type, 1, 1, 0L )  # uncompressed
  BlockTests(type, 1, 1, 40L)  # algorithm 1
  # BlockTests(type, 1, 1, 80L)  # algorithm 2

  lastBlock = as.integer((nrOfRows - 1) / blockSize)

  # Single last block
  BlockTests(type, lastBlock, lastBlock, 0L )  # uncompressed
  BlockTests(type, lastBlock, lastBlock, 40L)  # algorithm 1
  # BlockTests(type, lastBlock, lastBlock, 80L)  # algorithm 2

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
test_that("Integer column block tests",
{
  BlockTestSingleType("Xint")
})

test_that("Logical column block tests",
{
  BlockTestSingleType("Ylog")
})

blockSize = 2048

# Test blocks
test_that("Real column block tests",
{
  BlockTestSingleType("Zdoub")
})

blockSize = 2047

test_that("Character column block tests",
{
  BlockTestSingleType("Qchar")
})

test_that("Factor column block tests",
{
  BlockTestSingleType("WFact")
})

test_that("Character column block tests with NA's",
{
  BlockTestSingleType("CharNA")
})

test_that("Mixed columns are stored correctly",
{
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"))
})


test_that("From and to row can be set",
{
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), from = 10)
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), to = 8)
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), from = 4, to = 13)
})


test_that("Select columns",
{
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), selColumns = "Zdoub")
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), selColumns = c("Ylog", "WFact"))
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), selColumns = c("WFact", "Ylog"))
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact", "CharNA"), from = 7, to = 13, selColumns = c("Ylog", "Qchar"))
})


test_that("Select unknown column",
{
  expect_error(data <- fstread("FactorStore/data1.fds", columns = "bla"))
  expect_error(data <- fstread("FactorStore/data1.fds", columns = c("WFact", "bla")))
})


test_that("Select out of range row number",
{
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact"), from = 4, to = 7000)
  TestWriteRead(c("Xint", "Ylog", "Zdoub", "Qchar", "WFact"), from = 4, to = NULL)
  expect_error(fstread("FactorStore/data1.fst", from = 12000, to = NULL), "Row selection is out of range")
  expect_error(fstread("FactorStore/data1.fst", from = 0, to = NULL), "Parameter 'from' should have")
})
