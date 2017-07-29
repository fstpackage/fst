
context("OpenMP")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Get number of threads", {
  nrOfThreads <- fstgetthreads()
  expect_gte(nrOfThreads, 1)
  prevThreads <- fstsetthreads(2)  # Set number of OpenMP threads
  expect_equal(nrOfThreads, prevThreads)
  nrOfThreads <- fstgetthreads()

  # Systems with OpenMP activated should have more than a single thread
  if (fst:::hasopenmp()) {
    expect_equal(nrOfThreads, 2)
  } else {
    expect_equal(nrOfThreads, 1)
  }
})
