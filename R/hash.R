
#' Parallel calculation of the hash of a raw vector
#'
#' @param x raw vector that you want to hash
#' @param seed The seed value for the hashing algorithm. If NULL, a default seed will be used.
#'
#' @return hash value
#'
#' @export
hash_fst <- function(x, seed = NULL) {
  if (!is.null(seed) & ( (!is.numeric(seed)) | (length(seed) != 1))) {
    stop("Please specify an integer value for the hash seed.");
  }

  if (!is.raw(x)) {
    stop("Please specify a raw vector as input parameter x.");
  }

  fsthasher(x, as.integer(seed))
}
