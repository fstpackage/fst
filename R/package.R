
#' @useDynLib fst
#' @importFrom Rcpp sourceCpp
#' @import data.table
NULL


.onUnload <- function (libpath)
{
  library.dynam.unload("fst", libpath)
  data.table:::`[.data.table`
}
