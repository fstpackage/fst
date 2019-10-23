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


#' Read and write fst files.
#'
#' Read and write data frames from and to a fast-storage (`fst`) file.
#' Allows for compression and (file level) random access of stored data, even for compressed datasets.
#' Multiple threads are used to obtain high (de-)serialization speeds but all background threads are
#' re-joined before `write_fst` and `read_fst` return (reads and writes are stable).
#' When using a `data.table` object for `x`, the key (if any) is preserved,
#' allowing storage of sorted data.
#' Methods `read_fst` and `write_fst` are equivalent to `read.fst` and `write.fst` (but the
#' former syntax is preferred).
#'
#' @param x a data frame to write to disk
#' @param path path to fst file
#' @param compress value in the range 0 to 100, indicating the amount of compression to use.
#' Lower values mean larger file sizes. The default compression is set to 50.
#' @param uniform_encoding If `TRUE`, all character vectors will be assumed to have elements with equal encoding.
#' The encoding (latin1, UTF8 or native) of the first non-NA element will used as encoding for the whole column.
#' This will be a correct assumption for most use cases.
#' If `uniform.encoding` is set to `FALSE`, no such assumption will be made and all elements will be converted
#' to the same encoding. The latter is a relatively expensive operation and will reduce write performance for
#' character columns.
#' @return `read_fst` returns a data frame with the selected columns and rows. `write_fst`
#' writes `x` to a `fst` file and invisibly returns `x` (so you can use this function in a pipeline).
#' @examples
#' # Sample dataset
#' x <- data.frame(A = 1:10000, B = sample(c(TRUE, FALSE, NA), 10000, replace = TRUE))
#'
#' # Default compression
#' write_fst(x, "dataset.fst")  # filesize: 17 KB
#' y <- read_fst("dataset.fst") # read fst file
#'
#' # Maximum compression
#' write_fst(x, "dataset.fst", 100)  # fileSize: 4 KB
#' y <- read_fst("dataset.fst") # read fst file
#'
#' # Random access
#' y <- read_fst("dataset.fst", "B") # read selection of columns
#' y <- read_fst("dataset.fst", "A", 100, 200) # read selection of columns and rows
#' @export
#' @md
write_fst <- function(x, path, compress = 50, uniform_encoding = TRUE) {
  if (!is.character(path)) stop("Please specify a correct path.")

  if (!is.data.frame(x)) stop("Please make sure 'x' is a data frame.")

  dt <- fststore(normalizePath(path, mustWork = FALSE), x, as.integer(compress), uniform_encoding)

  if (inherits(dt, "fst_error")) {
    stop(dt)
  }

  return(invisible(x))
}


#' Read metadata from a fst file
#'
#' Method for checking basic properties of the dataset stored in \code{path}.
#'
#' @param path path to fst file
#' @param old_format must be FALSE, the old fst file format is deprecated and can only be read and
#' converted with fst package versions 0.8.0 to 0.8.10.
#' @return Returns a list with meta information on the stored dataset in \code{path}.
#' Has class \code{fstmetadata}.
#' @examples
#' # Sample dataset
#' x <- data.frame(
#'   First = 1:10,
#'   Second = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
#'   Last = sample(LETTERS, 10))
#'
#' # Write to fst file
#' write_fst(x, "dataset.fst")
#'
#' # Display meta information
#' metadata_fst("dataset.fst")
#' @export
metadata_fst <- function(path, old_format = FALSE) {

  if (old_format != FALSE) {
    stop("Parameter old_format is depricated, fst files written with fst package version",
      " lower than 0.8.0 should be read (and rewritten) using fst package versions <= 0.8.10.")
  }

  full_path <- normalizePath(path, mustWork = FALSE)

  metadata <- fstmetadata(full_path)

  if (inherits(metadata, "fst_error")) {
    stop(metadata)
  }

  col_info <- list(path = full_path, nrOfRows = metadata$nrOfRows,
    keys = metadata$keyNames, columnNames = metadata$colNames,
    columnBaseTypes = metadata$colBaseType, keyColIndex = metadata$keyColIndex,
    columnTypes = metadata$colType)
  class(col_info) <- "fstmetadata"

  col_info
}


#' @export
print.fstmetadata <- function(x, ...) {
  cat("<fst file>\n")
  cat(x$nrOfRows, " rows, ", length(x$columnNames), " columns (", basename(x$path),
    ")\n\n", sep = "")

  types <- c("unknown", "character", "factor", "ordered factor", "integer", "POSIXct", "difftime",
    "IDate", "ITime", "double", "Date", "POSIXct", "difftime", "ITime", "logical", "integer64",
    "nanotime", "raw")

  # table has no key columns
  if (is.null(x$keys)) {
    column_names <- format(encodeString(x$columnNames, quote = "'"))
    cat(paste0("* ", column_names, ": ", types[x$columnTypes], "\n"), sep = "")
    return(invisible(NULL))
  }

  # table has key columns
  keys <- data.frame(
    k = x$keys,
    count = seq_along(x$keys),
    stringsAsFactors = FALSE)

  col_info <- data.frame(
    k = x$columnNames,
    o = seq_along(x$columnNames),
    t = types[x$columnTypes],
    stringsAsFactors = FALSE)

  # merge keys to correct column
  col_info <- merge(col_info, keys, "k", all.x = TRUE, sort = FALSE)

  col_info$k <- format(encodeString(col_info$k, quote = "'"))
  col_info$l <- paste0(" (key ", col_info$count, ")")
  col_info[is.na(col_info$count), "l"] <- ""

  col_info <- col_info[order(col_info$count, col_info$o), ]  # key columns at the top

  cat(paste0("* ", col_info$k, ": ", col_info$t, col_info$l, "\n"), sep = "")
}


#' @rdname write_fst
#'
#' @param columns Column names to read. The default is to read all columns.
#' @param from Read data starting from this row number.
#' @param to Read data up until this row number. The default is to read to the last row of the stored dataset.
#' @param as.data.table If TRUE, the result will be returned as a \code{data.table} object. Any keys set on
#' dataset \code{x} before writing will be retained. This allows for storage of sorted datasets. This option
#' requires \code{data.table} package to be installed.
#' @param old_format must be FALSE, the old fst file format is deprecated and can only be read and
#' converted with fst package versions 0.8.0 to 0.8.10.
#'
#' @export
read_fst <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE, old_format = FALSE) {  # nolint
  file_name <- normalizePath(path, mustWork = FALSE)

  if (!is.null(columns)) {
    if (!is.character(columns)) {
      stop("Parameter 'columns' should be a character vector of column names.")
    }
  }

  if (!is.numeric(from) || from < 1 || length(from) != 1) {
    stop("Parameter 'from' should have a numerical value equal or larger than 1.")
  }

  from <- as.integer(from)

  if (!is.null(to)) {
    if (!is.numeric(to) || length(to) != 1) {
      stop("Parameter 'to' should have a numerical value larger than 1 (or NULL).")
    }

    to <- as.integer(to)
  }

  if (old_format != FALSE) {
    stop("Parameter old_format is depricated, fst files written with fst package version",
    " lower than 0.8.0 should be read (and rewritten) using fst package versions <= 0.8.10.")
  }

  res <- fstretrieve(file_name, columns, from, to)

  if (inherits(res, "fst_error")) {
    stop(res)
  }

  # long vectors are not supported yet with data.table, tibble's or data.frame,
  # so return a list instead
  nr_of_rows <- length(res$resTable[[1]])
  if (nr_of_rows >= 2 ^ 31) {
    return(res$resTable)
  }

  if (as.data.table) {
    if (!requireNamespace("data.table", quietly = TRUE)) {
      stop("Please install package data.table when using as.data.table = TRUE")
    }

    key_names <- res$keyNames
    res <- data.table::setDT(res$resTable)  # nolint
    if (length(key_names) > 0) data.table::setattr(res, "sorted", key_names)
    return(res)
  }

  res_table <- res$resTable

  # use setters from data.table to improve performance
  if (requireNamespace("data.table", quietly = TRUE)) {
    data.table::setattr(res_table, "class", "data.frame")
    data.table::setattr(res_table, "row.names", seq_len(length(res_table[[1L]])))
  } else {
    class(res_table) <- "data.frame"
    attr(res_table, "row.names") <- seq_len(length(res_table[[1L]]))
  }

  res_table
}


#' @rdname write_fst
#' @export
write.fst <- function(x, path, compress = 50, uniform_encoding = TRUE) {
  write_fst(x, path, compress, uniform_encoding)
}


#' @rdname write_fst
#' @export
read.fst <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE, old_format = FALSE) {  # nolint
  read_fst(path, columns, from, to, as.data.table, old_format)
}


#' @rdname metadata_fst
#' @export
fst.metadata <- function(path, old_format = FALSE) {  # nolint
  metadata_fst(path, old_format)
}
