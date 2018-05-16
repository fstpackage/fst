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


# load bit64 namespace when integer64 type column is read from file
require_bit64 <- function() {
  if (!requireNamespace("bit64", quietly = TRUE))
    warning("Some columns are type 'integer64' but package bit64 is not installed. ",
      "Those columns will print as strange looking floating point data. ",
      "There is no need to reload the data. Simply install.packages('bit64') to obtain ",
      "the integer64 print method and print the data again.")
}


# load data.table namespace when ITime type column is read from file
require_data_table <- function() {
  if (!requireNamespace("data.table", quietly = TRUE))
    warning("Some columns are type 'ITime' but package data.table is not installed. ",
     "Those columns will print incorrectly. There is no need to ",
     "reload the data. Simply install.packages('data.table') to obtain the data.table print ",
     "method and print the data again.")
}


require_nanotime <- function() {
  # called in print when they see nanotime columns are present
  if (!requireNamespace("nanotime", quietly = TRUE))
    warning("Some columns are type 'nanotime' but package nanotime is not installed. ",
      "Those columns will print as strange looking floating point data. There is no need to ",
      "reload the data. Simply install.packages('nanotime') to obtain the nanotime print ",
      "method and print the data again.")
}
