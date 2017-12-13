
library(testthat)
library(fst)
library(data.table)
library(lintr)
library(bit64)

source("testthat/helper_fstwrite.R")  # cross-version testing

test_check("fst")
