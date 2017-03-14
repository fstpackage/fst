
# Convenience method to tests on different versions of the package
# Not used in the CRAN release
fstwrite <- function(x, path, compress = 0)
{
  write.fst(x, path, compress)  # use current version of fst package
}


fstread <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE)
{
  read.fst(path, columns, from, to, as.data.table)
}


