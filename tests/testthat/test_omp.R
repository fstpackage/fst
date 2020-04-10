
context("OpenMP")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Get number of threads", {
  nr_of_threads <- threads_fst()
  expect_gte(nr_of_threads, 1)  # expect at least a single thread
  prev_threads <- threads_fst(2)  # Set number of OpenMP threads
  expect_equal(nr_of_threads, prev_threads)
  nr_of_threads <- threads_fst()

  # Systems with OpenMP activated should have more than a single thread
  if (fst:::hasopenmp()) {
    expect_equal(nr_of_threads, 2)
  } else {
    expect_equal(nr_of_threads, 1)
  }
})


test_that("threads_fst(0) use all logical cores", {
  threads_fst(0)
  nr_of_threads <- threads_fst()
  logical_cores <- parallel::detectCores(logical = TRUE)

  # Systems with OpenMP activated should have more than a single thread
  if (fst:::hasopenmp()) {
    expect_gt(nr_of_threads, 1)
  } else {
    expect_equal(nr_of_threads, 1)
  }
})


test_that("Loading fst works with options", {

  # Note that neither of the tests in this block will be informative when run
  # on a machine without openmp. They'll pass even if the thing they're testing
  # is broken.

  orig_op <- getOption("fst_threads")
  options(fst_threads = 1)

  # First test that .onload, which happens when the namespace is loaded by ::,
  # reads from the fst.threads option.
  fstcore:::.onLoad()
  nr_of_threads <- threads_fst()
  expect_equal(nr_of_threads, 1)

  # Next, test that subsequently attaching the package doesn't change
  # the number of threads.
  fstcore:::.onAttach()
  nr_of_threads <- threads_fst()
  expect_equal(nr_of_threads, 1)
  options(fst.threads = orig_op)  # reset option
})


test_that("Set reset_after_fork", {

  expect_error(threads_fst(reset_after_fork = 3), "Parameter reset_after_fork should be set")

})
