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
