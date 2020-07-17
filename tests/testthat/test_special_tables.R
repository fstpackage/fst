
context("special tables")


# Clean testdata directory
if (!file.exists("testdata")) {
  dir.create("testdata")
} else {
  file.remove(list.files("testdata", full.names = TRUE))
}


difftime_mode <- function(mode = "double") {
  vec <- (Sys.time() + 1) - Sys.time()
  mode(vec) <- mode
  vec[0]
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
  Difftime = difftime_mode(),
  DiffTime_int = difftime_mode("integer"),
  WFact = factor(sample(LETTERS, 26, replace = TRUE))[0],
  WFactNA = factor(NA)[0],
  stringsAsFactors = FALSE
)


test_that("zero row multi-column table", {

  # read-write cycle
  write_fst(datatable, "testdata/zero_row.fst")
  y <- read_fst("testdata/zero_row.fst")

  expect_equal(datatable, y)
})


test_that("zero-column table", {

  # read-write cycle
  write_fst(data.frame(), "testdata/zero_column.fst")
  y <- read_fst("testdata/zero_column.fst")

  expect_equal(data.frame(), y)
})
