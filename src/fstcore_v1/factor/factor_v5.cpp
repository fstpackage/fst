/*
  fst - An R-package for ultra fast storage and retrieval of datasets.
  Copyright (C) 2017, Mark AJ Klik

  BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

  * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

  * Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following disclaimer
    in the documentation and/or other materials provided with the
    distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact the author at :
  - fst source repository : https://github.com/fstPackage/fst
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

#include <blockstreamer_v1.h>

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
