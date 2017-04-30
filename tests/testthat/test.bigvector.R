
context("big vectors")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}

# # Sample data
# x <- list(X = rep(1.1, 500000000))
# setDT(x)
# gc()
#
# require(microbenchmark)
#
# test_that("Read meta of sorted file",
# {
#   microbenchmark(
#     write.fst(x, "testdata/big.fst"), times = 1)
#
#   rm(x)
#   gc()
#
#   microbenchmark(
#     y <- read.fst("testdata/big.fst", as.data.table = TRUE),
#     times = 1)
#
#   rm(y)
#   gc()
# })


