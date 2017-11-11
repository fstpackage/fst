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
