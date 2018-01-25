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
#' @importFrom Rcpp sourceCpp
#' @importFrom utils packageVersion
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
#'
#' **_The following copyright notice, list of conditions and disclaimer apply to
#' the use of the ZSTD library in the fst package:_**
#'
#' BSD License
#'
#' For Zstandard software
#'
#' Copyright (c) 2016-present, Facebook, Inc. All rights reserved.
#'
#' Redistribution and use in source and binary forms, with or without modification,
#' are permitted provided that the following conditions are met:
#'
#' * Redistributions of source code must retain the above copyright notice, this
#' list of conditions and the following disclaimer.
#'
#' * Redistributions in binary form must reproduce the above copyright notice,
#' this list of conditions and the following disclaimer in the documentation
#' and/or other materials provided with the distribution.
#'
#' * Neither the name Facebook nor the names of its contributors may be used to
#' endorse or promote products derived from this software without specific
#' prior written permission.
#'
#' THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#' ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#' WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#' DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
#' ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#' (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#' LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#' ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#' (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#' SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#'
#'
#' **_The following copyright notice, list of conditions and disclaimer apply to
#' the use of the LZ4 library in the fst package:_**
#'
#' LZ4 Library
#' Copyright (c) 2011-2016, Yann Collet
#' All rights reserved.
#'
#' Redistribution and use in source and binary forms, with or without modification,
#' are permitted provided that the following conditions are met:
#'
#'   * Redistributions of source code must retain the above copyright notice, this
#' list of conditions and the following disclaimer.
#'
#' * Redistributions in binary form must reproduce the above copyright notice, this
#' list of conditions and the following disclaimer in the documentation and/or
#' other materials provided with the distribution.
#'
#' THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#' ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#' WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#' DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
#' ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#' (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#'   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#' ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#' (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#' SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#'
#' @md
#' @docType package
#' @name fst-package
#' @aliases fst-package
NULL


.onUnload <- function (libpath) {
  library.dynam.unload("fst", libpath)
}
