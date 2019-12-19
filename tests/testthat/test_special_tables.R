
context("special tables")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


difftime_vec <- function(nr_of_rows, mode = "double") {
  vec <- (Sys.time() + 1:nr_of_rows) - Sys.time()
  mode(vec) <- mode
  vec
}


datatable <- data.frame(
  Xint = integer(0),
  Ylog = logical(0),
  Zdoub = double(0),
  Qchar = character(0),
  Ordered = ordered(sample(LETTERS, 26, replace = TRUE))[0],
  Date = as.Date("2019-01-01")[0],
  Integer64 = as.integer64(2345612345679)[0],
  Nanotime = nanotime(1000000)[0],
  Raw = as.raw(255)[0],
  Difftime = difftime_vec(1)[0],
  DiffTime_int = difftime_vec(1, "integer")[0],
  WFact = factor(sample(LETTERS, 26, replace = TRUE))[0],
  stringsAsFactors = FALSE
)


test_that("zero row multi-column table", {

  # read-write cycle
  write_fst(datatable, "testdata/zero_row.fst")
  y <- read_fst("testdata/zero_row.fst")

  expect_equal(datatable, y)
})
