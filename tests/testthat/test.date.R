
context("date column")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


# Note: probably crashes on non-openmp system
test_that("Type double Date issue #22 and #33", {
  u1 <- data.frame(DT = Sys.Date() - 1:10, variable = rep(c("A", "B"), 5), value = 1:10)
  fstwriteproxy(u1, "testdata/u1.fst")
  expect_equal(typeof(u1$DT), "double")  # double type date (expensive)

  u2 <- fstreadproxy("testdata/u1.fst")
  expect_equal(u1, u2)
  expect_equal(typeof(u2$DT), "double")  # remains double (no conversion to integer like in feather)
})


test_that("Include NA dates", {
  u1 <- data.frame(DT = as.Date("2010-01-01") + c(0L, 365L, NA))
  fstwriteproxy(u1, "testdata/u1.fst")
  expect_equal(typeof(u1$DT), "double")  # double type date (expensive)

  u2 <- fstreadproxy("testdata/u1.fst")
  expect_equal(u1, u2)
  expect_equal(typeof(u2$DT), "double")  # converted to integer type date
})


test_that("Include NA dates on integer Date vector", {
  u1 <- data.frame(DT = as.Date("2010-01-01") + c(0L, 365L, NA))
  u1$DT <- as.integer(u1$DT)
  class(u1$DT) <- "Date"

  fstwriteproxy(u1, "testdata/u1.fst")
  expect_equal(typeof(u1$DT), "integer")  # integer type date

  u2 <- fstreadproxy("testdata/u1.fst")
  expect_equal(typeof(u2$DT), "integer")  # still integer type date
  expect_equal(as.integer(u1$DT), as.integer(u2$DT))  # integer values identical

  expect_equal(class(u2$DT), c("IDate", "Date"))  # integer values identical
})
