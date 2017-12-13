
# Convenience method to tests on different versions of the package
# Not used in the CRAN release
fstwriteproxy <- function(x, path, compress = 0, uniform.encoding = TRUE) {
  write_fst(x, path, compress, uniform.encoding)  # use current version of fst package
}


fstreadproxy <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE) {
  read_fst(path, columns, from, to, as.data.table)
}


fstmetaproxy <- function(path) {
  metadata_fst(path)
}
