
context("OpenMP")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Check OpenMP support on Windows and Linux", {
  os_name <- Sys.info()['sysname']

  if (os_name == "Windows")  {
    expect_true(fst:::hasOpenMP())
  }

  if (os_name == "Linux") {
    expect_true(fst:::hasOpenMP())
  }

  cat("OS:", os_name, " OpenMp:", fst:::hasOpenMP(), sep = "")
})


test_that("Get number of threads", {
  os_name <- Sys.info()['sysname']

  if (os_name == "Windows" || os_name == "Linux") {
    nr_of_threads <- get_fst_threads()
    expect_gt(nr_of_threads, 1)

    prev_threads <- set_fst_threads(2)  # Set number of OpenMP threads
    expect_equal(nr_of_threads, prev_threads)

    nr_of_threads <- get_fst_threads()
    expect_equal(nr_of_threads, 2)
  }
})
