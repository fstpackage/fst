
context("OpenMP")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Get number of threads", {
  nrOfThreads <- threads_fst()
  expect_gte(nrOfThreads, 1)  # expect at least a single thread
  prevThreads <- threads_fst(2)  # Set number of OpenMP threads
  expect_equal(nrOfThreads, prevThreads)
  nrOfThreads <- threads_fst()

  # Systems with OpenMP activated should have more than a single thread
  if (fst:::hasopenmp()) {
    expect_equal(nrOfThreads, 2)
  } else {
    expect_equal(nrOfThreads, 1)
  }
})
