
#' Add rows to the data frame stored in a \code{fst} file.
#'
#' Take an existing \code{fst} file and append rows from a (in-memory) table.
#'
#' @param path Path to a \code{fst} file
#' @param x A data frame to append to an existing \code{fst} file.
#' @param compress Value in the range 0 to 100, indicating the amount of compression to use for the appended data frame.
#' If \code{NULL}, the compression setting of the existing \code{fst} file will be used.
fst_rbind <- function(path, x, compress = NULL) {
  filename <- normalizePath(path, mustWork = TRUE)

  if (!is.data.frame(x)) stop("Please make sure 'x' is a data frame.")

  stop("Not implemented yet")

  invisible(x)  # return a 'fst' object in the future
}
