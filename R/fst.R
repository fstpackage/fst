
#' Read and write fst files.
#'
#' Read and write data frames from and to a fast-storage (fst) file.
#' Allows for compression and (file level) random access of stored data, even for compressed datasets.
#' When using a \code{data.table} object for \code{x}, the key (if any) is preserved, allowing storage of sorted data.
#'
#' @param x A data frame to write to disk
#' @param path Path to fst file
#' @param compress Value in the range 0 to 100, indicating the amount of compression to use.
#' @return Both functions return a data frame. \code{write.fst}
#'   invisibly returns \code{x} (so you can use this function in a pipeline).
#' @examples
#' # Sample dataset
#' x <- data.frame(A = 1:10000, B = sample(c(TRUE, FALSE, NA), 10000, replace = TRUE))
#'
#' # Uncompressed
#' write.fst(x, "dataset.fst")  # filesize: 41 KB
#' read.fst("dataset.fst") # read uncompressed data
#'
#' # Compressed
#' write.fst(x, "dataset.fst", 100)  # fileSize: 4 KB
#' read.fst("dataset.fst") # read compressed data
#'
#' # Random access
#' read.fst("dataset.fst", "B") # read selection of columns
#' read.fst("dataset.fst", "A", 100, 200) # read selection of columns and rows
#' @export
write.fst <- function(x, path, compress = 0)
{
  if (!is.character(path)) stop("Please specify a correct path.")

  if (!is.data.table(x) & !(is.data.frame(x))) stop("Please make sure 'x' is a data frame.")

  fstStore(path, x, as.integer(compress))

  invisible(x)
}


#' Read metadata from a fst file
#'
#' Method for checking basic properties of the dataset stored in \code{path}.
#'
#' @param path Path to fst file
#' @return Returns A list with meta information on the stored dataset in \code{path}. Has class 'fst.metadata'.
#' @examples
#' # Sample dataset
#' x <- data.frame(
#'   First = 1:10,
#'   Second = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
#'   Last = sample(LETTERS, 10))
#'
#' # Write to fst file
#' write.fst(x, "dataset.fst")
#'
#' # Display meta information
#' fst.metadata("dataset.fst")
#' @export
fst.metadata <- function(path)
{
  metaData <- fstMeta(normalizePath(path, mustWork = TRUE))

  colInfo <- list(Path = shortPathName(path), NrOfRows = metaData$blockPosVec[1], Keys = metaData$keyColVec, ColumnNames = metaData$colNames, ColumnTypes = metaData$colTypeVec)
  class(colInfo) <- "fst.metadata"

  colInfo
}


#' @export
print.fst.metadata <- function(x, ...)
{
  cat("<fst file>\n")
  cat(x$NrOfRows, " rows, ", length(x$ColumnNames), " columns (", x$Path, ")\n\n", sep = "")

  types <- c("character", "integer", "real", "logical", "factor")
  colNames <- format(encodeString(x$ColumnNames, quote = "'"))

  cat(paste0("* ", colNames, ": ", types[x$ColumnTypes], "\n"), sep = "")
}


#' @rdname write.fst
#'
#' @param columns Column names to read. The default is to read all all columns.
#' @param from Read data starting from this row number.
#' @param to Read data up until this row number. The default is to read to the last row of the stored dataset.
#' @param as.data.table If TRUE, the result will be returned as a \code{data.table} object. Any keys set on
#' dataset \code{x} before writing, will be retained. This allows for storage of sorted datasets.
#'
#' @export
read.fst <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE)
{
  fileName <- normalizePath(path, mustWork = TRUE)

  if (!is.null(columns))
  {
    if(!is.character(columns))
    {
      stop("Parameter 'columns' should be a character vector of column names.")
    }
  }

  if(!is.numeric(from) || from < 1 || length(from) != 1)
  {
    stop("Parameter 'from' should have a numerical value equal or larger than 1.")
  }

  from <- as.integer(from)

  if (!is.null(to))
  {
    if(!is.numeric(to) || length(to) != 1)
    {
      stop("Parameter 'to' should have a numerical value larger than 1 (or NULL).")
    }

    to <- as.integer(to)
  }

  res <- fstRead(fileName, columns, from, to)

  if (as.data.table)
  {
    keyNames <- res$keyNames
    res <- as.data.table(res$resTable)
    attr(res, "sorted") <- keyNames
    return(res)
  }

  as.data.frame(res$resTable, row.names = NULL, stringsAsFactors = FALSE, optional = TRUE)
}

