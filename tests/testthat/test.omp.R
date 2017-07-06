
context("OpenMP")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Check OpenMP support on Windows and Linux", {
  os_name <- Sys.info()["sysname"]

  if (os_name == "Windows")  {
    expect_true(fst:::hasopenmp())
  }

  if (os_name == "Linux") {
    expect_true(fst:::hasopenmp())
  }
})


test_that("Get number of threads", {
  os_name <- Sys.info()["sysname"]

  if (os_name == "Windows" || os_name == "Linux") {
    nrOfThreads <- fstgetthreads()
    expect_gt(nrOfThreads, 1)

    prevThreads <- fstsetthreads(2)  # Set number of OpenMP threads
    expect_equal(nrOfThreads, prevThreads)

    nrOfThreads <- fstgetthreads()
    expect_equal(nrOfThreads, 2)
  }
})
