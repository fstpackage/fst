
context("factor column")


# Clean testdata directory
if (!file.exists("FactorStore")) {
  dir.create("FactorStore")
} else {
  file.remove(list.files("FactorStore", full.names = TRUE))
}


char_vec <- function(nrofrows) {
  sapply(1:nrofrows, function(x) {
    paste(sample(LETTERS, sample(1:4)), collapse = "")
  }
  ) }


factor_vec <- function(nrofrows, nroflevels) {
  levels <- NULL
  while (length(levels) < nroflevels) {
    levels <- unique(c(levels, char_vec(nroflevels)))
  }

  levels <- levels[1:nroflevels]

  factor(sample(levels, nrofrows, replace = TRUE), levels = levels)
}


sample_data <- function(nrofrows, nroflevels) {
  data.frame(WFact1 = factor_vec(nrofrows, nroflevels), WFact2 = factor_vec(nrofrows, nroflevels))
}


to_frame <- function(x) {
  data.frame(x, row.names = NULL, stringsAsFactors = FALSE)
}


test_write_read <- function(dt, offset = 3, cap = 3) {
  fstwriteproxy(dt, "FactorStore/data1.fst")

  # Read full dataset
  data <- fstreadproxy("FactorStore/data1.fst")
  expect_equal(dt, data)

  # Read with small offset
  data <- fstreadproxy("FactorStore/data1.fst", from = offset)
  expect_equal(to_frame(dt[offset:nrow(dt), , drop = FALSE]), data)

  # Read with medium offset
  data <- fstreadproxy("FactorStore/data1.fst", from = nrow(dt) - cap)
  expect_equal(to_frame(dt[(nrow(dt) - cap):nrow(dt), , drop = FALSE]), data)

  # Read less rows
  data <- fstreadproxy("FactorStore/data1.fst", to = cap)
  expect_equal(to_frame(dt[1:cap, , drop = FALSE]), data)

  # Read less rows
  data <- fstreadproxy("FactorStore/data1.fst", to = nrow(dt) - cap)
  expect_equal(to_frame(dt[1:(nrow(dt) - cap), , drop = FALSE]), data)

  # Read less rows with offset
  data <- fstreadproxy("FactorStore/data1.fst", from = offset, to = nrow(dt) - cap)
  expect_equal(to_frame(dt[offset:(nrow(dt) - cap), , drop = FALSE]), data)
}


test_that("Multiple sizes of 1-byte factor columns are stored correctly", {
  dt <- sample_data(30, 10)
  test_write_read(dt)
  test_write_read(dt[1:8, ])
  test_write_read(dt[1:7, ])
  # test large size here ?
})


test_that("Multiple sizes of 2-byte factor columns are stored correctly", {
  dt <- sample_data(30, 257)
  test_write_read(dt)
  test_write_read(dt[1:4, ], 2, 2)
  test_write_read(dt[1:3, ], 1, 1)
  # test large size here ?
})


# with thanks to @martinblostein for tracking the corresponding bug
# see: https://github.com/fstpackage/fst/issues/128
test_that("length 1 factor column with 2 byte level vector is stored correctly", {
  df <- data.frame(a = factor("X1", levels = paste0("X", 1:128)))
  write_fst(df, "FactorStore/one_row.fst")
  x <- read_fst("FactorStore/one_row.fst")

  expect_equal(df, x)
})


test_that("Multiple sizes of 4-byte factor columns  are stored correctly", {
  dt <- sample_data(30, 70000)
  test_write_read(dt)
  test_write_read(dt[1:8, ])
  test_write_read(dt[1:7, ])
  # test large size here ?
})


test_that("Small factor with a single NA level", {
  dt <- data.frame(A = 1:3, B = as.factor(rep(NA, 3)))
  fstwriteproxy(dt, "FactorStore/data1.fst")
  expect_equal(fstreadproxy("FactorStore/data1.fst"), dt)
})


test_that("Single block one-column factor with a single NA level", {
  dt <- data.frame(B = as.factor(rep(NA, 10)))
  test_write_read(dt)
})


test_that("Single block factor with a single NA level", {
  dt <- data.frame(A = 1:1000, B = as.factor(rep(NA, 10)))
  test_write_read(dt)
})


test_that("Medium factor with a single NA level", {
  dt <- data.frame(A = 1:10000, B = as.factor(rep(NA, 10000)))
  test_write_read(dt)
})
