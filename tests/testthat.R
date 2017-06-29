
require(testthat)
require(fst)
require(data.table)
require(bit)
require(bit64)
require(lintr)

source("testthat/helper.fstwrite.R")  # cross-version testing

test_check("fst")
