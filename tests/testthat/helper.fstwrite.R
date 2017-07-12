
# Convenience method to tests on different versions of the package
# Not used in the CRAN release
fstwriteproxy <- function(x, path, compress = 0, uniform.encoding = TRUE) {
  fstwrite(x, path, compress, uniform.encoding)  # use current version of fst package
}


fstreadproxy <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE) {
  fstread(path, columns, from, to, as.data.table)
}


fstmetaproxy <- function(path) {
  fstmeta(path)
}
