
#' @useDynLib fst, .registration = TRUE
#' @importFrom Rcpp sourceCpp
#' @importFrom utils packageVersion
#' @importFrom parallel detectCores
NULL


.onUnload <- function (libpath) {
  library.dynam.unload("fst", libpath)
}
