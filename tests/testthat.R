
# required packages
library(testthat)
library(fst)
library(data.table)
library(lintr)
library(bit64)
library(nanotime)

# some helper functions
source("testthat/helper_fstwrite.R")  # cross-version testing

# run tests
test_check("fst")
