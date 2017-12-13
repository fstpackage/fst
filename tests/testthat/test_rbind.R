
context("row binding")


# Clean testdata directory
if (!file.exists("testoutput")) {
  dir.create("testoutput")
} else {
  file.remove(list.files("testoutput", full.names = TRUE))
}


nr_of_levels <- 8
char_vec <- function(nr_of_rows) {
  sapply(1:nr_of_rows,
    function(x) {
      paste(sample(LETTERS, sample(1:4)), collapse = "")
    })
  }

# Sample data
nr_of_rows <- 10000L
char_na <- char_vec(nr_of_rows)
char_na[sample(1:nr_of_rows, 10)] <- NA
datatable <- data.frame(
  Xint = 1:nr_of_rows,
  Ylog = sample(c(TRUE, FALSE, NA), nr_of_rows, replace = TRUE),
  Zdoub = rnorm(nr_of_rows),
  Qchar = char_vec(nr_of_rows),
  WFact = factor(sample(char_vec(nr_of_levels), nr_of_rows, replace = TRUE)),
  char_na = char_na,
  stringsAsFactors = FALSE)
