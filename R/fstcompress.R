
#' Compress any object using the LZ4 or ZSTD compressor.
#'
#' @param x object to be serialized.
#' @param compressor compressor to use for compressing \code{x}. Valid options are "LZ4" (default) and "ZSTD".
#' @param compression compression factor used. Must be in the range 0 (lowest compression) to 100 (maximum compression).
#' @export
fstcompress <- function(x, compressor = "LZ4", compression = 0) {
  if (!is.numeric(compression)) {
    stop("Parameter compression should be a numeric value in the range 0 to 100")
  }

  if (!is.character(compressor)) {
    stop("Parameter compressor should be set to 'LZ4' or 'ZSTD'.")
  }

  if (!is.raw(x)) {
    stop("Parameter x is not set to a raw vector.")
  }

  fstcomp(x, compressor, as.integer(compression))
}
