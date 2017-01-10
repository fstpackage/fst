
#' @useDynLib fst
#' @importFrom Rcpp sourceCpp
#' @importFrom data.table is.data.table as.data.table
#' @importFrom utils shortPathName
NULL


.onUnload <- function (libpath)
{
  library.dynam.unload("fst", libpath)
}
