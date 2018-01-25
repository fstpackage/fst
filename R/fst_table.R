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


#' Access a fst file like a regular data frame
#'
#' Create a fst_table object that can be accessed like a regular data frame. This object
#' is just a reference to the actual data and requires only a small amount of memory.
#' When data is accessed, only the requested subset is read from file. This is possible
#' because the fst file format allows full random access (in columns and rows) to the stored
#' dataset.
#'
#' @inheritParams metadata_fst
#' @return An object of class \code{fst_table}
#' @export
#' @examples
#' # generate a sample fst file
#' path <- paste0(tempfile(), ".fst")
#' fst::write_fst(iris, path)
#'
#' # create a fst_table object that can be used as a data frame
#' ft <- fst(path)
#'
#' # print head and tail
#' print(ft)
#'
#' # select columns and rows
#' x <- ft[10:14, c("Petal.Width", "Species")]
#'
#' # use the common list interface
#' ft[TRUE]
#' ft[c(TRUE, FALSE)]
#' ft[["Sepal.Length"]]
#' ft$Petal.Length
#'
#' # use data frame generics
#' nrow(ft)
#' ncol(ft)
#' dim(ft)
#' dimnames(ft)
#' colnames(ft)
#' rownames(ft)
#' names(ft)
fst <- function(path, old_format = FALSE) {

  # wrap in a list so that additional elements can be added if required
  ft <- list(
    meta = metadata_fst(path, old_format),
    col_selection = NULL,
    row_selection = NULL,
    old_format = old_format
  )

  # class attribute
  class(ft) <- "fst_table"

  ft
}


#' @export
row.names.fst_table <- function(x) {
  as.character(seq_len(length(.subset2(x, "meta")$columnBaseTypes)))
}


#' @export
dim.fst_table <- function(x) {
  c(.subset2(x, "meta")$nrOfRows, length(.subset2(x, "meta")$columnBaseTypes))
}


#' @export
dimnames.fst_table <- function(x) {
  list(as.character(seq_len(.subset2(x, "meta")$nrOfRows)),
    .subset2(x, "meta")$columnNames)
}


#' @export
names.fst_table <- function(x) {
  .subset2(x, "meta")$columnNames
}


#' @export
print.fst_table <- function(x, ...) {
  print(.subset2(x, "meta"))
}


# Subsetting implementation for x[i, j] syntax
.subset_table <- function(x, i, j) {


  # no column selection done yet
  if (is.null(x$col_selection)) {

    # check for existing column name
    if (!(j %in% x$meta$columnNames)) {
      stop("Unknown column '", j, "'", call. = FALSE)
    }

    x$col_selection <- j

    return(x)
  }

  # there is a selection already
}


#' @export
`[[.fst_table` <- function(x, j, exact = TRUE) {
  if (!exact) {
    warning("exact ignored", call. = FALSE)
  }

  meta_info <- .subset2(x, "meta")

  if (length(j) != 1) {

    # select at least one column number
    if (length(j) == 0) {
      stop("Please use a length one integer or character vector to specify the column", call. = FALSE)
    }

    # recursive indexing (up to level 1)

    if (length(j) != 2) {
      stop("Recursive indexing is currently only supported up to level 1.", call. = FALSE)
    }

    if (is.character(j)) {
      stop("Subscript out of bounds.", call. = FALSE)
    }

    # check row number

    if (j[2] < 1 || j[2] > meta_info$nrOfRows) {
      stop("Second index out of bounds.", call. = FALSE)
    }

    col_name <- meta_info$columnNames[as.integer(j[1])]

    return(read_fst(meta_info$path, col_name, from = j[2], to = j[2],
      old_format = .subset2(x, "old_format"))[[1]])
  }

  if (!(is.numeric(j) || is.character(j))) {
    stop("Please use a length 1 integer of character vector to specify the column", call. = FALSE)
  }

  # length one integer column selection
  if (is.numeric(j)) {
    if (j < 1 || j > length(meta_info$columnNames)) {
      stop("Invalid column index ", j, call. = FALSE)
    }

    j <- meta_info$columnNames[as.integer(j)]
  } else {
    if (!(j %in% meta_info$columnNames)) {
      return(NULL)
    }
  }

  # determine row selection here from metadata

  read_fst(meta_info$path, j, old_format = .subset2(x, "old_format"))[[1]]
}


# override needed to avoid the [[ operator messing up the str output

#' @export
str.fst_table <- function(object, ...) {
  str(unclass(object))
}


#' @export
`$.fst_table` <- function(x, j) {
  x[[j]]
}


#' @export
print.fst_table <- function(x, ...) {
  meta_info <- .subset2(x, "meta")
  cat("<fst file>\n")
  cat(meta_info$nrOfRows, " rows, ", length(meta_info$columnNames),
      " columns (", basename(meta_info$path), ")\n\n", sep = "")

  sample_data_head <- read_fst(meta_info$path, from = 1, to = 5, old_format = .subset2(x, "old_format"))
  sample_data_tail <- read_fst(meta_info$path, from = meta_info$nrOfRows - 4, to = meta_info$nrOfRows,
    old_format = .subset2(x, "old_format"))

  colnames(sample_data_tail) <- rep("", length(meta_info$columnNames))

  print(sample_data_head, row.names = FALSE)
  cat("---")
  print(sample_data_tail, row.names = FALSE)

  invisible(x)
}


#' @export
as.data.frame.fst_table <- function(x, row.names = NULL, optional = FALSE, ...) {
  meta_info <- .subset2(x, "meta")
  as.data.frame(read_fst(meta_info$path, old_format = .subset2(x, "old_format")), row.names, optional, ...)
}

#' @export
as.list.fst_table <- function(x, ...) {
  as.list(as.data.frame(x))
}


.column_indexes_fst <- function(meta_info, j) {

  # test correct column names
  if (is.character(j) ) {
    wrong <- !(j %in% meta_info$columnNames)

    if (any(wrong)) {
      names <- j[wrong]
      stop(sprintf("Undefined columns: %s", paste(names, collapse = ", ")))
    }
  } else if (is.logical(j)) {

    if (any(is.na(j))) {
      stop("Subscript out of bounds.", call. = FALSE)
    }

    j <- meta_info$columnNames[j]
  } else if (is.numeric(j)) {
    if (any(is.na(j))) {
      stop("Subscript out of bounds.", call. = FALSE)
    }

    if (any(j <= 0)) {
      stop("Only positive column indexes supported.", call. = FALSE)
    }

    if (any(j > length(meta_info$columnBaseTypes))) {
      stop("Subscript out of bounds.", call. = FALSE)
    }

    j <- meta_info$columnNames[j]
  }

  j
}


#' @export
`[.fst_table` <- function(x, i, j, drop = FALSE) {
  if (drop) {
    warning("drop ignored", call. = FALSE)
  }

  meta_info <- .subset2(x, "meta")

  # when only i is present, we do a column subsetting

  if (missing(i) && missing(j)) {
    return(read_fst(meta_info$path, old_format = .subset2(x, "old_format")))
  }

  if (nargs() <= 2) {

    # return full table
    if (missing(i)) {
      # we have a j
      j <- .column_indexes_fst(meta_info, j)
      return(read_fst(meta_info$path, j, old_format = .subset2(x, "old_format")))
    }

    # i is interpreted as j
    j <- .column_indexes_fst(meta_info, i)
    return(read_fst(meta_info$path, j, old_format = .subset2(x, "old_format")))
  }

  # return all rows

  # special case where i is interpreted as j: select all rows
  if (nargs() == 3 && !missing(drop) && !missing(i)) {
    j <- .column_indexes_fst(meta_info, i)
    return(read_fst(meta_info$path, j, old_format = .subset2(x, "old_format")))
  }

  # i and j not reversed

  # full columns
  if (missing(i)) {
    j <- .column_indexes_fst(meta_info, j)
    return(read_fst(meta_info$path, j, old_format = .subset2(x, "old_format")))
  }


  # determine integer vector from i
  if (!(is.numeric(i) || is.logical(i))) {
    stop("Row selection should be done using a numeric or logical vector", call. = FALSE)
  }

  if (is.logical(i)) {
    i <- which(i)
  }

  # cast to integer and determine row range
  i <- as.integer(i)
  min_row <- min(i)
  max_row <- max(i)

  # boundary check
  if (min_row < 0) {
    stop("Row selection out of range")
  }

  if (max_row > meta_info$nrOfRows) {
    stop("Row selection out of range")
  }

  # column subset

  # select all columns
  if (missing(j)) {
    fst_data <- read_fst(meta_info$path, from = min_row, to = max_row, old_format = .subset2(x, "old_format"))

    return(fst_data[1 + i - min_row, ])
  }

  j <- .column_indexes_fst(meta_info, j)
  fst_data <- read_fst(meta_info$path, j, from = min_row, to = max_row, old_format = .subset2(x, "old_format"))

  fst_data[1 + i - min_row, ]
}
