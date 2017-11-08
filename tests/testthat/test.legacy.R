
context("legacy format")

# nolint start
#
# # Clean testdata directory
# if (!file.exists("testdata")) {
#   dir.create("testdata")
# } else {
#   file.remove(list.files("testdata", full.names = TRUE))
# }
#
#
# # Create a pool of strings
# nroflevels <- 8
#
# char_vec <- function(nrofrows) {
#   sapply(1:nrofrows, function(x) {
#     paste(sample(LETTERS, sample(1:4)), collapse = "")
#   }
#   ) }
#
# char_veclong <- function(nrofrows) {
#   sapply(1:nrofrows,
#     function(x) {
#       paste(sample(LETTERS, sample(20:25)), collapse = "")
#     }
#     ) }
#
# date_vec <- function(nrofrows) {
#   date_vec <- sample(1:nrofrows, replace = TRUE)
#   class(date_vec) <- c("IDate", "Date")
#   date_vec
# }
#
# difftime_vec <- function(nrOfrows, mode = "double") {
#   vec <- (Sys.time() + 1:nrofrows) - Sys.time()
#   mode(vec) <- mode
#   vec
# }
#
# # Sample data
# nrofrows <- 1000L
# char_na <- char_vec(nrofrows)
# char_na[sample(1:nrofrows, 10)] <- NA
# datatable <- data.frame(
#   Xint = 1:nrofrows,
#   Ylog = sample(c(TRUE, FALSE, NA), nrofrows, replace = TRUE),
#   Zdoub = rnorm(nrofrows),
#   Qchar = char_vec(nrofrows),
#   WFact = factor(sample(char_vec(nroflevels), nrofrows, replace = TRUE)),
#   Ordered = ordered(sample(char_vec(nroflevels), nrofrows, replace = TRUE)),
#   char_na = char_na,
#   CharLong = char_veclong(nrofrows),
#   Date = date_vec(nrofrows),
#   DateDouble = as.Date("2015-01-01") + 1:nrofrows,
#   Difftime = difftime_vec(nrOfrows),
#   DiffTime_int = difftime_vec(nrOfrows, "integer"),
#   stringsAsFactors = FALSE)
#
# require(fst)
#
# write.fst(datatable, "datasets/legacy.fst")
# x <- read.fst("datasets/legacy.fst")
# saveRDS(x, "datasets/legacy.rds")
#
# nolint end


test_that("Read legacy format", {

  # only test on little endian platforms
  if (.Platform$endian == "little" && .Platform$OS.type == "windows") {
    expect_error(
      read_fst("datasets/legacy.fst"),
      "File header information does not contain the fst format marker"
    )

    expect_warning(
      dt <- read_fst("datasets/legacy.fst", old_format = TRUE),
      "This fst file was created with a beta version of the fst package. Please"
    )

    dt_old <- readRDS("datasets/legacy.rds")

    expect_equal(dt, dt_old)

    expect_error(
      metadata_fst("datasets/legacy.fst"),
      "File header information does not contain the fst format marker"
    )

    expect_warning(
      res <- capture_output(print(metadata_fst("datasets/legacy.fst", old_format = TRUE))),
        "This fst file was created with a beta version of the fst package. Please"
    )

    expect_equal(res, paste(
      "<fst file>\n1000 rows, 12 columns (datasets/legacy.fst)\n",
      "* 'Xint'        : integer",
      "* 'Ylog'        : logical",
      "* 'Zdoub'       : double",
      "* 'Qchar'       : character",
      "* 'WFact'       : factor",
      "* 'Ordered'     : factor",
      "* 'char_na'     : character",
      "* 'CharLong'    : character",
      "* 'Date'        : integer",
      "* 'DateDouble'  : double",
      "* 'Difftime'    : double",
      "* 'DiffTime_int': integer",
      sep = "\n"))
  }
})
