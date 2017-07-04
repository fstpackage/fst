
#' Get or set the number of threads used in parallel operations
#'
#' For parallel operations, the performance is determined to a great extend by the number of threads
#' used. More threads will allow the CPU to perform more computational intensive tasks simultaneously,
#' speeding up the operation. Using more threads also introduces some overhead that will scale with the
#' number of threads used. Therefore, using the maximum number of available threads is not always the
#' fastest solution. With \code{set_fst_threads} the number of threads can be adjusted to the users
#' specific requirements. As a default, \code{fst} uses a number of threads equal to the number of
#' physical cores in the system (not the number of logical cores).
#'
#' @return The number of threads used (\code{fstgetthreads}) or the previous number of threads used
#' (\code{fstsetthreads}).
#' @export
fstgetthreads <- function() {
  return(getNrOfActiveThreads())
}

#' @rdname fstgetthreads
#' @param nrOfThreads number of threads to use
#' @export
fstsetthreads <- function(nrOfThreads) {
  return(setNrOfActiveThreads(nrOfThreads))
}
