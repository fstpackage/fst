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


#' @useDynLib fst, .registration = TRUE
#' @import Rcpp
#' @importFrom fstcore threads_fstlib
#' @importFrom utils packageVersion
#' @importFrom utils capture.output
#' @importFrom utils tail
#' @importFrom utils str
#' @importFrom parallel detectCores
NULL


#' Lightning Fast Serialization of Data Frames for R.
#'
#' Multithreaded serialization of compressed data frames using the 'fst' format.
#' The 'fst' format allows for random access of stored data which can be compressed with the
#' LZ4 and ZSTD compressors.
#'
#' The fst package is based on three C++ libraries:
#'
#' * **fstlib**: library containing code to write, read and compute on files stored in the _fst_ format.
#' Written and owned by Mark Klik.
#' * **LZ4**: library containing code to compress data with the LZ4 compressor. Written and owned
#' by Yann Collet.
#' * **ZSTD**: library containing code to compress data with the ZSTD compressor. Written by
#' Yann Collet and owned by Facebook, Inc.
#'
#' As of version 0.9.8, these libraries are included in the fstcore package, on which fst depends.
#' The copyright notices of the above libraries can be found in the fstcore package.
#'
#' @md
#' @docType package
#' @name fst-package
#' @aliases fst-package
NULL
