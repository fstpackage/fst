
#' @useDynLib fst
#' @importFrom Rcpp sourceCpp
#' @importFrom data.table is.data.table as.data.table
NULL


.onUnload <- function (libpath)
{
  library.dynam.unload("fst", libpath)
}
