% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/package.R
\docType{package}
\name{fst-package}
\alias{fst-package}
\title{Lightning Fast Serialization of Data Frames for R.}
\description{
Multithreaded serialization of compressed data frames using the 'fst' format.
The 'fst' format allows for random access of stored data which can be compressed with the
LZ4 and ZSTD compressors.
}
\details{
The fst package is based on three C++ libraries:
\itemize{
\item \strong{fstlib}: library containing code to write, read and compute on files stored in the \emph{fst} format.
Written and owned by Mark Klik.
\item \strong{LZ4}: library containing code to compress data with the LZ4 compressor. Written and owned
by Yann Collet.
\item \strong{ZSTD}: library containing code to compress data with the ZSTD compressor. Written by
Yann Collet and owned by Facebook, Inc.
}

As of version 0.9.8, these libraries are included in the fstcore package, on which fst depends.
The copyright notices of the above libraries can be found in the fstcore package.
}
