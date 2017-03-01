

test_that("Reading non-existent file gives an error",
{
  expect_error(read.fst("AccessStore/non-existent.fst"))
})


test_that("Writing to incorrect path gives an error",
{
  expect_error(write.fst(data.frame(A = 1:10), "AccessStore/A/non-existent.fst"))
})


test_that("Dataset needs at least one column",
{
  expect_error(write.fst(data.frame(), "bla.fst"), "Your dataset needs at least one column")
})

test_that("Dataset needs at least one column",
{
  expect_error(write.fst(data.frame(A = integer(0)), "bla.fst"), "The dataset contains no data")
})
