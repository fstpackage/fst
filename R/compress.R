#  fst - R package for ultra fast storage and retrieval of datasets
#
#  Copyright (C) 2017-present, Mark AJ Klik
#
#  This file is part of the fst R package.
#
#  The fst R package is free software: you can redistribute it and/or modify it
#  under the terms of the GNU Affero General Public License version 3 as
#  published by the Free Software Foundation.
#
#  The fst R package is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
#  for more details.
#
#  You should have received a copy of the GNU Affero General Public License along
#  with the fst R package. If not, see <http://www.gnu.org/licenses/>.
#
#  You can contact the author at:
#  - fst R package source repository : https://github.com/fstpackage/fst


#' Compress a raw vector using the LZ4 or ZSTD compressor.
#'
#' @param x raw vector.
#' @param compressor compressor to use for compressing \code{x}. Valid options are "LZ4" and "ZSTD" (default).
#' @param compression compression factor used. Must be in the range 0 (lowest compression) to 100 (maximum compression).
#' @param hash Compute hash of compressed data. This hash is stored in the resulting raw vector and
#' can be used during decompression to check the validity of the compressed vector. Hash
#' computation is done with the very fast xxHash algorithm and implemented as a parallel operation,
#' so the performance hit will be very small.
#'
#' @export
compress_fst <- function(x, compressor = "ZSTD", compression = 0, hash = FALSE) {
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
