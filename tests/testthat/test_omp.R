
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

test_that("threads_fst(0) use all logical cores", {
  threads_fst(0)
  nrOfThreads <- threads_fst()
  logical_cores <- parallel::detectCores(logical = TRUE)
  expect_equal(nrOfThreads, logical_cores)
})

test_that("Loading fst works with options", {
  skip("Currently causes a segfault")
  # Note that neither of the tests in this block will be informative when run
  # on a machine without openmp. They'll pass even if the thing they're testing
  # is broken.

  # I'm not sure how to get testthat to test the effects of .onLoad and
  # .onAttach. Here was my attempt, but the code below causes a segfault.

  unloadNamespace("fst")
  orig_op <- getOption("fst.threads")
  options(fst.threads = 1)
  # First test that .onload, which happens when the namespace is loaded by ::,
  # reads from the fst.threads option.
  loadNamespace("fst")
  nrOfThreads <- fst::threads_fst()
  expect_equal(nrOfThreads, 1)
  # Next, test that using attaching the package doesn't change
  # the number of threads.
  attachNamespace("fst")
  nrOfThreads <- threads_fst()
  expect_equal(nrOfThreads, 1)
  options(fst.threads = orig_op)  # reset option
})
