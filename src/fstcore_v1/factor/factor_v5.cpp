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


// Standard headers
#include <iostream>
#include <fstream>

#include <Rcpp.h>

// Framework headers
#include <character/character_v1.h>
#include <integer/integer_v2.h>
#include <factor/factor_v5.h>
#include <compression/compression.h>
#include <compression/compressor.h>
#include <blockstreamer/blockstreamer_v1.h>

using namespace std;
using namespace Rcpp;


// Parameter 'startRow' is zero based.
SEXP fdsReadFactorVec_v5(ifstream &myfile, SEXP &intVec, unsigned long long blockPos, unsigned int startRow,
  unsigned int length, unsigned int size)
{
  // Jump to factor level
  myfile.seekg(blockPos);

  // Get vector meta data
  char meta[12];
  myfile.read(meta, 12);
  unsigned int *nrOfLevels = (unsigned int*) meta;
  unsigned long long* levelVecPos = (unsigned long long*) &meta[4];

  // Read level strings
  SEXP strVec;
  PROTECT(strVec = Rf_allocVector(STRSXP, *nrOfLevels));
  SEXP singleColInfo = fdsReadCharVec_v1(myfile, strVec, blockPos + 12, 0, *nrOfLevels, *nrOfLevels);  // get level strings

  // Read level values
  char* values = (char*) INTEGER(intVec);  // output vector
  SEXP intVecInfo = fdsReadColumn_v1(myfile, values, *levelVecPos, startRow, length, size, 4);

  Rf_setAttrib(intVec, Rf_mkString("levels"), strVec);
  Rf_setAttrib(intVec, Rf_mkString("class"), Rf_mkString("factor"));

  return List::create(
    _["singleColInfo"] = singleColInfo,
    _["intVecInfo"] = intVecInfo,
    _["strVec"] = strVec);
}
