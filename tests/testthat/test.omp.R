
context("OpenMP")

source("helper.fstwrite.R")

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Check OpenMP support on Windows and Linux",
{
  osName <- Sys.info()['sysname']

  if (osName == "Windows")
  {
    expect_true(fst:::hasOpenMP())
  }

  if (osName == "Linux")
  {
    expect_true(fst:::hasOpenMP())
  }

  cat("OS:", osName, " OpenMp:", fst:::hasOpenMP(), sep = "")
})


test_that("Get number of threads",
{
  osName <- Sys.info()['sysname']

  if (osName == "Windows" || osName == "Linux")
  {
    nrOfThreads <- getFstThreads()
    expect_gt(nrOfThreads, 1)

    prevThreads <- setFstThreads(2)  # Set number of OpenMP threads
    expect_equal(nrOfThreads, prevThreads)

    nrOfThreads <- getFstThreads()
    expect_equal(nrOfThreads, 2)
  }
})

