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


#include <Rcpp.h>

#include <fstcore.h>


// [[Rcpp::export]]
SEXP getnrofthreads()
{
  return fstcore::getnrofthreads();
}


// [[Rcpp::export]]
int setnrofthreads(SEXP nrOfThreads)
{
  return fstcore::setnrofthreads(nrOfThreads);
}


// [[Rcpp::export]]
void restore_after_fork(bool restore)
{
  fstcore::restore_after_fork(restore);
}


// [[Rcpp::export]]
SEXP hasopenmp()
{
  return fstcore::hasopenmp();
}

