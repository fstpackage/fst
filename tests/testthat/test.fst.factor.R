
context("factor column")


# Clean testdata directory
if (!file.exists("FactorStore"))
{
  dir.create("FactorStore")
} else
{
  file.remove(list.files("FactorStore", full.names = TRUE))
}


CharVec <- function(nrOfRows) { sapply(1:nrOfRows, function(x) { paste(sample(LETTERS, sample(1:4)), collapse="") }) }
FactorVec <- function(nrOfRows, nrOfLevels)
{
  levels <- NULL
  while (length(levels) < nrOfLevels)
  {
    levels <- unique(c(levels, CharVec(nrOfLevels)))
  }

  levels <- levels[1:nrOfLevels]

  factor(sample(levels, nrOfRows, replace = TRUE), levels = levels)
}


SampleData <- function(nrOfRows, nrOfLevels)
{
  data.frame(WFact1 = FactorVec(nrOfRows, nrOfLevels), WFact2 = FactorVec(nrOfRows, nrOfLevels))
}


ToFrame <- function(x)
{
  data.frame(x, row.names = NULL, stringsAsFactors = FALSE)
}


TestWriteRead <- function(dt, offset = 3, cap = 3)
{
  fstwrite(dt, "FactorStore/data1.fst")

  # Read full dataset
  data <- fstread("FactorStore/data1.fst")
  expect_equal(dt, data)

  # Read with small offset
  data <- fstread("FactorStore/data1.fst", from = offset)
  expect_equal(ToFrame(dt[offset:nrow(dt),]), data)

  # Read with medium offset
  data <- fstread("FactorStore/data1.fst", from = nrow(dt) - cap)
  expect_equal(ToFrame(dt[(nrow(dt) - cap):nrow(dt),]), data)

  # Read less rows
  data <- fstread("FactorStore/data1.fst", to = cap)
  expect_equal(ToFrame(dt[1:cap,]), data)

  # Read less rows
  data <- fstread("FactorStore/data1.fst", to = nrow(dt) - cap)
  expect_equal(ToFrame(dt[1:(nrow(dt) - cap),]), data)

  # Read less rows with offset
  data <- fstread("FactorStore/data1.fst", from = offset, to = nrow(dt) - cap)
  expect_equal(ToFrame(dt[offset:(nrow(dt) - cap),]), data)
}


test_that("Multiple sizes of 1-byte factor columns  are stored correctly",
{
  dataTable <- SampleData(30, 10)
  TestWriteRead(dataTable)
  TestWriteRead(dataTable[1:8, ])
  TestWriteRead(dataTable[1:7, ])
  # test large size here ?
})


test_that("Multiple sizes of 2-byte factor columns  are stored correctly",
{
  dataTable <- SampleData(30, 257)
  TestWriteRead(dataTable)
  TestWriteRead(dataTable[1:4, ], 2, 2)
  TestWriteRead(dataTable[1:3, ], 1, 1)
  # test large size here ?
})


test_that("Multiple sizes of 4-byte factor columns  are stored correctly",
{
  dataTable <- SampleData(30, 70000)
  TestWriteRead(dataTable)
  TestWriteRead(dataTable[1:8, ])
  TestWriteRead(dataTable[1:7, ])
  # test large size here ?
})



