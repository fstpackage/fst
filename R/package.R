
#' @useDynLib fst
#' @importFrom Rcpp sourceCpp
#' @import data.table
#' @importFrom utils packageVersion
#' @importFrom parallel detectCores
NULL


.onUnload <- function (libpath) {
  library.dynam.unload("fst", libpath)
}
