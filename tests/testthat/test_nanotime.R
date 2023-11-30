
context("nanotime column")


library(data.table)


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


test_that("Cycle return the nanotime type", {
  skip_if_not(requireNamespace("nanotime", quietly = TRUE))

  # Prepare example
  dt_nanotime <- data.frame(
    Nano = nanotime::nanotime(Sys.Date() + 1:100)
  )

  expect_true(inherits(dt_nanotime$Nano, "nanotime"))

  # Write to fst
  fstwriteproxy(dt_nanotime, "testdata/dt_nanotime.fst")
  dt_nanotime_read <- fstreadproxy("testdata/dt_nanotime.fst")

  # nanotime type preserved:
  expect_true(inherits(dt_nanotime_read$Nano, "nanotime"))

  expect_identical(dt_nanotime, dt_nanotime_read)
})
