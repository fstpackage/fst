
#' Get the number of threads use by all OpenMP parallel calculations in \code{fst}
#'
#' @return The number of threads used by OpenMP.
#' @export
getFstThreads <- function()
{
  return(getNrOfActiveThreads())
}


#' Set the number of threads to use for parallel calculations in \code{fst}
#'
#' @param nrOfThreads Number of threads to use for parallel calculations
#'
#' @return The previous number of threads set.
#' @export
setFstThreads <- function(nrOfThreads)
{
  return(setNrOfActiveThreads(nrOfThreads))
}
