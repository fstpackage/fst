
context("date column")

# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}

test_that("Type double Date issue #22 and #33",
{
  u1 <- data.frame(DT = Sys.Date() - 1:10, variable = rep(c("A", "B"), 5), value = 1:10)
  fstwrite(u1, "testdata/u1.fst")
  expect_equal(typeof(u1$DT), "double")  # double type date (expensive)

  u2 <- fstread("testdata/u1.fst")
  expect_equal(u1, u2)
  expect_equal(typeof(u2$DT), "integer")  # converted to integer type date
})
