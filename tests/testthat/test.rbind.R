
context("row binding")


# Clean testdata directory
if (!file.exists("testoutput"))
{
  dir.create("testoutput")
} else
{
  file.remove(list.files("testoutput", full.names = TRUE))
}


nrOfLevels <- 8
CharVec <- function(nrOfRows) { sapply(1:nrOfRows, function(x) { paste(sample(LETTERS, sample(1:4)), collapse="") }) }

# Sample data
nrOfRows <- 10000L
charNA <- CharVec(nrOfRows)
charNA[sample(1:nrOfRows, 10)] <- NA
dataTable <- data.frame(
  Xint=1:nrOfRows,
  Ylog=sample(c(TRUE, FALSE, NA), nrOfRows, replace=TRUE),
  Zdoub=rnorm(nrOfRows),
  Qchar=CharVec(nrOfRows),
  WFact=factor(sample(CharVec(nrOfLevels), nrOfRows, replace = TRUE)),
  CharNA = charNA,
  stringsAsFactors = FALSE)


# test_that("Use rbind to bind rows",
# {
#   write.fst(dataTable[1:1000, ], "testoutput/1.fst")
#
#   fst.rbind("testoutput/1.fst", dataTable[1001:2000, ])
# })
