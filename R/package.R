
#' @useDynLib fst
#' @importFrom Rcpp sourceCpp
#' @importFrom data.table is.data.table setDT
NULL


.onUnload <- function (libpath)
{
  library.dynam.unload("fst", libpath)
}
