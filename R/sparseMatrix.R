#' @export
write_fst.sparseMatrix <- function(x, path, compress = 50, uniform_encoding = TRUE) {
  if (!is.character(path)) stop("Please specify a correct path.")

  classes <- is(x)

  if (any(classes %in% 'dsparseMatrix')) indices <- c('i', 'p', 'x')
  else indices <- c('i', 'p')

  dt <- fststore(normalizePath(path, mustWork = FALSE), as.data.frame(attributes(x)[indices]), as.integer(compress), uniform_encoding)

  if (inherits(dt, "fst_error")) {
    stop(dt)
  }

  return(invisible(x))
}
