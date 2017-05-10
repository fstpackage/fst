
#' @useDynLib fst
#' @importFrom Rcpp sourceCpp
#' @import data.table
#' @importFrom utils packageVersion
NULL


.onUnload <- function (libpath)
{
  library.dynam.unload("fst", libpath)
}
