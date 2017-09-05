
context("nanotime column")

library(nanotime)
library(data.table)

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


# Prepare example
dt_nanotime <- data.frame(
  Nano = nanotime(Sys.Date() + 1:100))


test_that("Cycle return the nanotime type", {
  expect_true(inherits(dt_nanotime$Nano, "nanotime"))
})
