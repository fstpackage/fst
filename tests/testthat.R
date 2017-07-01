
library(testthat)
library(fst)
library(data.table)
library(bit64)
# library(lintr)  # lintr disabled

source("testthat/helper.fstwrite.R")  # cross-version testing

test_check("fst")
