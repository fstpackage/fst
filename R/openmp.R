
#' Get or set the number of threads used in parallel operations
#'
#' For parallel operations, the performance is determined to a great extend by the number of threads
#' used. More threads will allow the CPU to perform more computational intensive tasks simultaneously,
#' speeding up the operation. Using more threads also introduces some overhead that will scale with the
#' number of threads used. Therefore, using the maximum number of available threads is not always the
#' fastest solution. With \code{threads_fst} the number of threads can be adjusted to the users
#' specific requirements. As a default, \code{fst} uses a number of threads equal to the number of
#' physical cores in the system (not the number of logical cores).
#'
#' @param nr_of_threads number of threads to use or \code{NULL} to get the current number of threads used in
#' multi-threaded operations.
#'
#' @return the number of threads (previously) used
#' @export
threads_fst <- function(nr_of_threads = NULL) {
  if (is.null(nr_of_threads)) {
    return(getnrofthreads())
  }

  setnrofthreads(nr_of_threads)
}
