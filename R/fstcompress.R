
#' Compress any object using the LZ4 or ZSTD compressor.
#'
#' @param x object to be serialized.
#' @param compressor compressor to use for compressing \code{x}. Valid options are "LZ4" (default) and "ZSTD".
#' @param compression compression factor used. Must be in the range 0 (lowest compression) to 100 (maximum compression).
fstcompress <- function(x, compressor = "LZ4", compression = 0) {
  rawVec <- serialize(x, NULL)

  rawVec
}
