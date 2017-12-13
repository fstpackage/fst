/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  fstlib is free software: you can redistribute it and/or modify it under the
  terms of the GNU Affero General Public License version 3 as published by the
  Free Software Foundation.

  fstlib is distributed in the hope that it will be useful, but WITHOUT ANY
  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
  A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
  details.

  You should have received a copy of the GNU Affero General Public License
  along with fstlib. If not, see <http://www.gnu.org/licenses/>.

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/


// Standard libraries
#include <ostream>
#include <istream>

#include <Rcpp.h>

#include <compression/compression.h>
#include <compression/compressor.h>
#include <blockstreamer/blockstreamer_v1.h>


using namespace std;
using namespace Rcpp;

SEXP fdsReadRealVec_v3(ifstream &myfile, SEXP &realVec, unsigned long long blockPos, unsigned startRow, unsigned length, unsigned size)
{
  char* values = (char*) REAL(realVec);  // output vector

  return fdsReadColumn_v1(myfile, values, blockPos, startRow, length, size, 8);
}
