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

#include <Rcpp.h>

#include <fstream>

#include <fstrbind.h>
#include <fstmetadata.h>
#include <fstdefines.h>


using namespace Rcpp;
using namespace std;


// [[Rcpp::export]]
SEXP BytesConvert(SEXP integer)
{
  int* intVal = INTEGER(integer);
  unsigned int uVal = (unsigned int) intVal[0];

  vector<int> result(32);
  vector<int> resultU(32);

  for (int count = 0; count < 32; ++count)
  {
    result[count] = (intVal[0] >> (31 - count)) & 1;
    resultU[count] = (uVal >> (31 - count)) & 1;
  }

  int toInt= (int) uVal;

  return List::create(
    _["bytes"] = result,
    _["bytesU"] = resultU,
    _["uval"] = uVal,
    _["toInt"] = toInt);
}

SEXP fstrbind(String fileName, SEXP table, SEXP compression, Function serializer)
{
  SEXP colNames = Rf_getAttrib(table, R_NamesSymbol);
  SEXP keyNames = Rf_getAttrib(table, Rf_mkString("sorted"));


  // get fst file metadata

  // fst file stream using a stack buffer
  ifstream myfile;
  char ioBuf[4096];
  myfile.rdbuf()->pubsetbuf(ioBuf, 4096);
  myfile.open(fileName.get_cstring(), ios::binary);

  if (myfile.fail())
  {
    myfile.close();
    ::Rf_error("There was an error opening the fst file, please check for a correct path.");
  }

  // Read variables from fst file header
  FstMetaData metaData;

  unsigned int tableClassType;
  int keyLength, nrOfColsFirstChunk;
  unsigned int version = metaData.ReadHeader(myfile, tableClassType, keyLength, nrOfColsFirstChunk);

  // We can't append to a fst file with version lower than 1
  if (version == 0)
  {
    myfile.close();
    ::Rf_error("Can't append to this version of the fst format. Please update the fst package to a newer version.");
  }

  uint64_t firstHorzChunkPos = 24 + 4 * keyLength;

  istream* fstfile  = static_cast<istream*>(&myfile);
  metaData.Collect(*fstfile, firstHorzChunkPos);

  // Assume a data.table

  // Fix: check for identical key

  // Pseudo code:

//
//   FstTable fstTable;
//   FstColumnFactory colFact;
//
//   for (int colCount = 0; colCount < nrOfCOls; ++colCount)
//   {
//     FstColumn fstColumn = colFact.Create
//
//     fstTable.AddColumn(fstColumn);
//   }

  // check for equal column types

  // set compression or copy from existing meta data

  // Seek to last horizontal chunk index

  // Seek to last vertical chunk

  // Add chunk

  // Update vertical chunk index of lat horizontal chunk

  return List::create(
    _["colNames"] = colNames,
    _["keyNames"] = keyNames,
    _["version"] = version,
    _["lastHorzChunkPointer"] = metaData.lastHorzChunkPointer,
    _["colTypeVec"] = metaData.colTypeVec,
    _["colNamesFST"] = metaData.colNameVec,
    _["colAttrVec"] = metaData.colAttrVec,
    _["firstHorzChunkPos"] = firstHorzChunkPos);
}
