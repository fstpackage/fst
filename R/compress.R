
#' Compress a raw vector using the LZ4 or ZSTD compressor.
#'
#' @param x raw vector.
#' @param compressor compressor to use for compressing \code{x}. Valid options are "LZ4" (default) and "ZSTD".
#' @param compression compression factor used. Must be in the range 0 (lowest compression) to 100 (maximum compression).
#' @param hash Compute hash of compressed data. This hash is stored in the resulting raw vector and
#' can be used during decompression to check the validity of the compressed vector. Hash
#' computation is done with the very fast xxHash algorithm and implemented as a parallel operation,
#' so the performance hit will be very small.
#'
#' @export
compress_fst <- function(x, compressor = "LZ4", compression = 0, hash = FALSE) {
  if (!is.numeric(compression)) {
    stop("Parameter compression should be a numeric value in the range 0 to 100")
  }

  if (!is.character(compressor)) {
    stop("Parameter compressor should be set to 'LZ4' or 'ZSTD'.")
  }

  if (!is.raw(x)) {
    stop("Parameter x is not set to a raw vector.")
  }

  compressed_vec <- fstcomp(x, compressor, as.integer(compression), hash)

  if (inherits(compressed_vec, "fst_error")) {
    stop(compressed_vec)
  }

  compressed_vec
}


#' Decompress a raw vector with compressed data.
#'
#' @param x raw vector with data previously compressed with \code{compress_fst}.
#'
#' @return a raw vector with previously compressed data.
#' @export
decompress_fst <- function(x) {

  if (!is.raw(x)) {
    stop("Parameter x should be a raw vector with compressed data.")
  }

  decompressed_vec <- fstdecomp(x)

  if (inherits(decompressed_vec, "fst_error")) {
    stop(decompressed_vec)
  }

  decompressed_vec
}
