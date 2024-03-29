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
#' When data is accessed, only a subset is read from file, depending on the minimum and
#' maximum requested row number. This is possible because the fst file format allows full
#' random access (in columns and rows) to the stored dataset.
#'
#' @inheritParams metadata_fst
#' @return An object of class \code{fst_table}
#' @export
#' @examples
#' \dontrun{
#' # generate a sample fst file
#' path <- paste0(tempfile(), ".fst")
#' write_fst(iris, path)
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
#' }
fst <- function(path, old_format = FALSE) {
  # old format is deprecated as of v0.9.0
  if (old_format != FALSE) {
    stop(
      "Parameter old_format is depricated, fst files written with fst package version",
      " lower than 0.8.0 should be read (and rewritten) using fst package versions <= 0.8.10."
    )
  }

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
  list(
    as.character(seq_len(.subset2(x, "meta")$nrOfRows)),
    .subset2(x, "meta")$columnNames
  )
}


#' @export
names.fst_table <- function(x) {
  .subset2(x, "meta")$columnNames
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

    return(
      read_fst(
        meta_info$path,
        col_name,
        from = j[2],
        to = j[2],
        old_format = .subset2(x, "old_format")
      )[[1]]
    )
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
  str(unclass(object), ...)
}


#' @export
`$.fst_table` <- function(x, j) {
  x[[j]]
}


#' @export
print.fst_table <- function(x, number_of_rows = 50, ...) {
  meta_info <- .subset2(x, "meta")

  cat("<fst file>\n")
  cat(meta_info$nrOfRows, " rows, ", length(meta_info$columnNames),
    " columns (", basename(meta_info$path), ")\n\n",
    sep = ""
  )

  if (!is.numeric(number_of_rows)) number_of_rows <- 100L
  if (!is.infinite(number_of_rows)) number_of_rows <- as.integer(number_of_rows)
  if (number_of_rows <= 0L) {
    return(invisible())
  } # ability to turn off printing

  table_splitted <- (meta_info$nrOfRows > number_of_rows) && (meta_info$nrOfRows > 10)

  if (table_splitted) {
    sample_data_head <- read_fst(meta_info$path, from = 1, to = 5, old_format = .subset2(x, "old_format"))
    sample_data_tail <- read_fst(meta_info$path,
      from = meta_info$nrOfRows - 4, to = meta_info$nrOfRows,
      old_format = .subset2(x, "old_format")
    )

    sample_data <- rbind.data.frame(sample_data_head, sample_data_tail)
  } else {
    sample_data <- read_fst(meta_info$path, old_format = .subset2(x, "old_format"))
  }

  # use bit64 package if available for correct printing
  if ((!"bit64" %in% loadedNamespaces()) && any(sapply(sample_data, inherits, "integer64"))) require_bit64() # nolint
  if ((!"nanotime" %in% loadedNamespaces()) && any(sapply(sample_data, inherits, "nanotime"))) require_nanotime() # nolint
  if ((!"data.table" %in% loadedNamespaces()) && any(sapply(sample_data, inherits, "ITime"))) require_data_table() # nolint

  types <- c(
    "unknown", "character", "factor", "ordered factor", "integer", "POSIXct", "difftime",
    "IDate", "ITime", "double", "Date", "POSIXct", "difftime", "ITime", "logical", "integer64",
    "nanotime", "raw"
  )

  # turn off colored output at default
  color_on <- FALSE

  if (requireNamespace("crayon", quietly = TRUE)) {
    color_on <- crayon::has_color() # terminal has color
  }

  type_row <- matrix(paste("<", types[meta_info$columnTypes], ">", sep = ""), nrow = 1)
  colnames(type_row) <- meta_info$columnNames

  # convert to aligned character columns
  sample_data_print <- format(sample_data)

  if (table_splitted) {
    dot_row <- matrix(rep("--", length(meta_info$columnNames)), nrow = 1)
    colnames(dot_row) <- meta_info$columnNames

    sample_data_print <- rbind(
      type_row,
      sample_data_print[1:5, , drop = FALSE],
      dot_row,
      sample_data_print[6:10, , drop = FALSE]
    )

    rownames(sample_data_print) <- c(" ", 1:5, "--", (meta_info$nrOfRows - 4):meta_info$nrOfRows)

    y <- capture.output(print(sample_data_print))

    # the length of y must be a multiple of 13 and color must be permitted
    # if not, take defensive action; skip coloring and solve later
    if (!color_on || (length(y) %% 13 != 0)) {
      print(sample_data_print)
      return(invisible(x))
    }

    type_rows <- seq(2, length(y), 13)
    gray_rows <- seq(8, length(y), 13)

    y[gray_rows] <- paste0("\033[38;5;248m", y[gray_rows], "\033[39m")

    gray_rows <- c(type_rows, gray_rows)
  } else {
    # table is not splitted along the row axis
    sample_data_print <- rbind(
      type_row,
      sample_data_print
    )

    rownames(sample_data_print) <- c(" ", seq_len(meta_info$nrOfRows))

    # no color terminal available
    if (!color_on) {
      print(sample_data_print)
      return(invisible(x))
    }

    y <- capture.output(print(sample_data_print))

    gray_rows <- type_rows <- seq(2, length(y), 2 + meta_info$nrOfRows)
  }

  # type rows are set to italic light grey
  y[type_rows] <- paste0("\033[3m\033[38;5;248m", y[type_rows], "\033[39m\033[23m")

  # use light grey color up to width of row name column
  row_text_size <- regexpr("^[0-9-]*", tail(y, 1))
  row_text_size <- attr(row_text_size, "match.length")

  y[-gray_rows] <- paste0(
    "\033[38;5;248m", substr(y[-gray_rows], 1, row_text_size),
    "\033[39m", substr(y[-gray_rows], row_text_size + 1, nchar(y[-gray_rows]))
  )

  cat(y, sep = "\n")
  return(invisible(x))
}


#' @export
as.data.frame.fst_table <- function(x, row.names = NULL, optional = FALSE, ...) { # nolint
  meta_info <- .subset2(x, "meta")
  as.data.frame(read_fst(meta_info$path, old_format = .subset2(x, "old_format")), row.names, optional, ...)
}

#' @export
as.list.fst_table <- function(x, ...) {
  as.list(as.data.frame(x))
}


.column_indexes_fst <- function(meta_info, j) {
  # test correct column names
  if (is.character(j)) {
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


# drop to lower dimension when drop = TRUE
return_drop <- function(x, drop) {
  if (!drop || ncol(x) > 1) {
    return(x)
  }

  x[[1]]
}


#' @export
`[.fst_table` <- function(x, i, j, drop) {
  # check for old_format in case an 'old' fst_table object was deserialized
  if (.subset2(x, "old_format") != FALSE) {
    stop(
      "fst files written with fst package version",
      " lower than 0.8.0 should be read (and rewritten) using fst package versions <= 0.8.10."
    )
  }

  meta_info <- .subset2(x, "meta")

  # no additional arguments provided

  if (missing(i) && missing(j)) {
    # never drop as with data.frame
    return(read_fst(meta_info$path))
  }


  if (nargs() <= 2) {
    # result is never dropped with 2 arguments

    if (missing(i)) {
      # we have a named argument j
      j <- .column_indexes_fst(meta_info, j)
      return(read_fst(meta_info$path, j))
    }

    # i is interpreted as j
    j <- .column_indexes_fst(meta_info, i)
    return(read_fst(meta_info$path, j))
  }

  # drop dimension if single column selected and drop != FALSE
  drop_dim <- FALSE

  if (!missing(j) && length(j) == 1) {
    if (!(!missing(drop) && drop == FALSE)) {
      drop_dim <- TRUE
    }
  }

  # special case where i is interpreted as j: select all rows, never drop

  if (nargs() == 3 && !missing(drop) && !missing(i)) {
    j <- .column_indexes_fst(meta_info, i)
    return(read_fst(meta_info$path, j))
  }

  # i and j not reversed

  # full columns
  if (missing(i)) {
    j <- .column_indexes_fst(meta_info, j)
    x <- read_fst(meta_info$path, j)

    if (!drop_dim) {
      return(x)
    }
    return(x[[1]])
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

  # empty row selection
  if (length(i) == 0) {
    min_row <- 1
    max_row <- 1
  } else {
    min_row <- min(i)
    max_row <- max(i)

    # boundary check
    if (min_row < 0) {
      stop("Row selection out of range")
    }

    if (max_row > meta_info$nrOfRows) {
      stop("Row selection out of range")
    }
  }

  # column subset

  # select all columns
  if (missing(j)) {
    fst_data <- read_fst(meta_info$path, from = min_row, to = max_row)
    x <- fst_data[1 + i - min_row, ] # row selection, no dropping
  } else {
    j <- .column_indexes_fst(meta_info, j)
    fst_data <- read_fst(meta_info$path, j, from = min_row, to = max_row)
    x <- fst_data[1 + i - min_row, , drop = FALSE] # row selection, no dropping
  }

  if (!drop_dim) {
    return(x)
  }
  return(x[[1]])
}
