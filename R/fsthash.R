
#' Parallel calculation of the hash of a raw vector
#'
#' @param x raw vector that you want to hash
#' @param seed The seed value for the hashing algorithm. If NULL, a default seed will be used.
#' @param block_size minimum block size to use for hashing.
#'
#' @return hash value
#'
#' @export
fsthash <- function(x, seed = NULL, block_size = 1024) {
  if (!is.null(seed) & ( (!is.numeric(seed)) | (length(seed) != 1))) {
    stop("Please specify an integer value for the hash seed.");
  }

  if (!is.numeric(block_size) & (length(block_size) != 1)) {
    stop("Please specify a (positive) integer value for the block_size.");
  }

  if (!is.raw(x)) {
    stop("Please specify a raw vector as input parameter x.");
  }

  fsthasher(x, as.integer(seed), as.integer(block_size))
}
