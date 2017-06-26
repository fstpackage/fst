
require(testthat)
require(fst)
require(data.table)
library(bit64)

source("testthat/helper.fstwrite.R")  # cross-version testing

test_check("fst")
