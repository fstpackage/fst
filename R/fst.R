
#' Read and write fst files.
#'
#' Read and write data frames from and to a fast-storage (fst) file.
#' Allows for compression and (file level) random access of stored data, even for compressed datasets.
#' When using a \code{data.table} object for \code{x}, the key (if any) is preserved,
#' allowing storage of sorted data.
#' Methods \code{fstread} and \code{fstwrite} are equivalent to \code{read.fst} and \code{write.fst} (but the
#' former syntax is preferred).
#'
#' @param x a data frame to write to disk
#' @param path path to fst file
#' @param compress value in the range 0 to 100, indicating the amount of compression to use.
#' @param uniform.encoding If TRUE, all character vectors will be assumed to have elements with equal encoding.
#' The encoding (latin1, UTF8 or native) of the first non-NA element will used as encoding for the whole column.
#' This will be a correct assumption for most use cases.
#' If \code{uniform.encoding} is set to FALSE, no such assumption will be made and all elements will be converted
#' to the same encoding. The latter is a relatively expensive operation and will reduce write performance for
#' character columns.
#' @return all methods return a data frame. \code{fstwrite}
#' invisibly returns \code{x} (so you can use this function in a pipeline).
#' @examples
#' # Sample dataset
#' x <- data.frame(A = 1:10000, B = sample(c(TRUE, FALSE, NA), 10000, replace = TRUE))
#'
#' # Uncompressed
#' fstwrite(x, "dataset.fst")  # filesize: 41 KB
#' y <- fstread("dataset.fst") # read uncompressed data
#'
#' # Compressed
#' fstwrite(x, "dataset.fst", 100)  # fileSize: 4 KB
#' y <- fstread("dataset.fst") # read compressed data
#'
#' # Random access
#' y <- fstread("dataset.fst", "B") # read selection of columns
#' y <- fstread("dataset.fst", "A", 100, 200) # read selection of columns and rows
#' @export
fstwrite <- function(x, path, compress = 0, uniform.encoding = TRUE) {
  if (!is.character(path)) stop("Please specify a correct path.")

  if (!is.data.frame(x)) stop("Please make sure 'x' is a data frame.")

  fststore(normalizePath(path, mustWork = FALSE), x, as.integer(compress), uniform.encoding)

  invisible(x)
}


#' @rdname fstwrite
#' @export
write.fst <- function(x, path, compress = 0, uniform.encoding = TRUE) {
  fstwrite(x, path, compress, uniform.encoding)
}


#' Read metadata from a fst file
#'
#' Method for checking basic properties of the dataset stored in \code{path}.
#'
#' @param path path to fst file
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
#' fstwrite(x, "dataset.fst")
#'
#' # Display meta information
#' fstmeta("dataset.fst")
#' @export
fstmeta <- function(path) {
  metadata <- fstmetadata(normalizePath(path, mustWork = TRUE))

  colInfo <- list(path = path, nrOfRows = metadata$nrOfRows,
    keys = metadata$keyNames, columnNames = metadata$colNames,
    columnBaseTypes = metadata$colBaseType, keyColIndex = metadata$keyColIndex,
    columnTypes = metadata$colType)
  class(colInfo) <- "fstmetadata"

  colInfo
}


#' @rdname fstmeta
#' @export
fst.metadata <- function(path) {
  fstmeta(path)
}


#' @export
print.fstmetadata <- function(x, ...) {
  cat("<fst file>\n")
  cat(x$nrOfRows, " rows, ", length(x$columnNames), " columns (", x$path,
    ")\n\n", sep = "")

  types <- c("unknown", "character", "factor", "integer", "POSIXct", "IDate", "double", "Date", "POSIXct", "logical",
    "integer64", "nanotime", "POSIXct", "raw")

  colNames <- format(encodeString(x$columnNames, quote = "'"))

  # Table has no key columns
  if (is.null(x$keys)) {
    cat(paste0("* ", colNames, ": ", types[x$columnTypes], "\n"), sep = "")
    return(invisible(NULL))
  }

  # Table has key columns
  keys <- data.frame(k = x$keys, count = 1:length(x$keys))
  colTab <- data.frame(k = x$columnNames, o = 1:length(x$columnNames))
  colTab <- merge(colTab, keys, "k", all.x = TRUE)
  colTab$l <- paste0(" (key ", colTab$count, ")")
  colTab[is.na(colTab$count), "l"] <- ""

  cat(paste0("* ", colNames, ": ", types[x$columnTypes], colTab$l, "\n"), sep = "")
}


#' @rdname fstwrite
#'
#' @param columns Column names to read. The default is to read all all columns.
#' @param from Read data starting from this row number.
#' @param to Read data up until this row number. The default is to read to the last row of the stored dataset.
#' @param as.data.table If TRUE, the result will be returned as a \code{data.table} object. Any keys set on
#' dataset \code{x} before writing, will be retained. This allows for storage of sorted datasets.
#'
#' @export
fstread <- function(path, columns = NULL, from = 1, to = NULL,
  as.data.table = FALSE) {
  fileName <- normalizePath(path, mustWork = TRUE)

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

  res <- fstretrieve(fileName, columns, from, to)

  if (as.data.table) {
    if (!requireNamespace("data.table")) {
      stop("Please install package data.table when using as.data.table = TRUE")
    }

    keyNames <- res$keyNames
    res <- data.table::setDT(res$resTable)  # nolint
    if (length(keyNames) > 0 ) attr(res, "sorted") <- keyNames
    return(res)
  }

  as.data.frame(res$resTable, row.names = NULL, stringsAsFactors = FALSE,
    optional = TRUE)
}


#' @rdname fstwrite
#' @export
read.fst <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE) {

  fstread(path, columns, from, to, as.data.table)
}
