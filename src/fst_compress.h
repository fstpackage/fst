/*
 fst - R package for ultra fast storage and retrieval of datasets

 Copyright (C) 2017-present, Mark AJ Klik

 This file is part of the fst R package.

 The fst R package is free software: you can redistribute it and/or modify it
 under the terms of the GNU Affero General Public License version 3 as
 published by the Free Software Foundation.

 The fst R package is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 for more details.

 You should have received a copy of the GNU Affero General Public License along
 with the fst R package. If not, see <http://www.gnu.org/licenses/>.

 You can contact the author at:
 - fst R package source repository : https://github.com/fstpackage/fst
*/


#ifndef FSTCOMPRESS_H
#define FSTCOMPRESS_H


#include <Rcpp.h>


// [[Rcpp::export]]
SEXP fsthasher(SEXP rawVec, SEXP seed, SEXP blockHash);


// [[Rcpp::export]]
SEXP fstcomp(SEXP rawVec, SEXP compressor, SEXP compression, SEXP hash, SEXP r_container);


// [[Rcpp::export]]
SEXP fstdecomp(SEXP rawVec, SEXP r_container);


#endif  // FASTSTORE_H
