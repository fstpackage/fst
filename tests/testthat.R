
# required packages
library(testthat)
library(fst)

# some helper functions
source("testthat/helper_fstwrite.R")  # cross-version testing

# run tests
test_check("fst")
