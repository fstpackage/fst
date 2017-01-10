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

// Framework headers
#include "factStore.h"
#include "blockStore.h"
#include "lowerbound.h"
#include <compression.h>
#include <compressor.h>
#include <intStore.h>
#include <charStore.h>

// Standard headers
#include <iostream>
#include <fstream>

// External libraries
#include "lz4.h"
// #include <boost/unordered_map.hpp>

using namespace std;
using namespace Rcpp;


SEXP fdsWriteFactorVec(ofstream &myfile, SEXP &factVec, unsigned size, unsigned int compression)
{
  SEXP res;  // timing information
  unsigned long long blockPos = myfile.tellp();  // offset for factor

  // Vector meta data
  char meta[12];
  unsigned int* nrOfLevels = (unsigned int*) &meta;
  unsigned long long* levelVecPos = (unsigned long long*) &meta[4];
  myfile.write(meta, 12);  // number of levels

  SEXP levelVec = Rf_getAttrib(factVec, Rf_mkString("levels"));
  *nrOfLevels = LENGTH(levelVec);

  // Store levels here

  fdsWriteCharVec(myfile, levelVec, *nrOfLevels, compression);

  // Rewrite meta-data
  *levelVecPos = myfile.tellp();  // offset for level vector

  myfile.seekp(blockPos);
  myfile.write(meta, 12);  // number of levels
  myfile.seekp(*levelVecPos);  // return to end of file

  // Store factor vector here

  int* intP = INTEGER(factVec);  // data pointer
  unsigned int nrOfRows = LENGTH(factVec);  // vector length

  // With zero compression only a fixed width compactor is used (int to byte or int to short)

  if (compression == 0)
  {
    if (*nrOfLevels < 128)
    {
      FixedRatioCompressor* compressor = new FixedRatioCompressor(CompAlgo::INT_TO_BYTE);  // compression level not relevant here
      res = fdsStreamUncompressed(myfile, (char*) intP, nrOfRows, 4, BLOCKSIZE_INT, compressor);
      delete compressor;

      // Error here in output file?

      return List::create(
        _["*nrOfLevels"] = *nrOfLevels,
        _["*levelVecPos"] = *levelVecPos,
        _["*levelVec"] = levelVec,
        _["compression"] = compression,
        _["blockPos"] = blockPos,
        _["nrOfRows"] = nrOfRows,
        _["res"] = res);
    }

    if (*nrOfLevels < 32768)
    {
      FixedRatioCompressor* compressor = new FixedRatioCompressor(CompAlgo::INT_TO_SHORT);  // compression level not relevant here
      res = fdsStreamUncompressed(myfile, (char*) intP, nrOfRows, 4, BLOCKSIZE_INT, compressor);
      delete compressor;

      return res;
    }

    return fdsStreamUncompressed(myfile, (char*) intP, nrOfRows, 4, BLOCKSIZE_INT, NULL);
  }

  int blockSize = 4 * BLOCKSIZE_INT;  // block size in bytes

  if (*nrOfLevels < 128)  // use 1 byte per int
  {
    Compressor* defaultCompress = NULL;
    Compressor* compress2 = NULL;
    StreamCompressor* streamCompressor = NULL;

    if (compression <= 50)
    {
      defaultCompress = new SingleCompressor(CompAlgo::INT_TO_BYTE, 0);  // compression not relevant here
      compress2 = new SingleCompressor(CompAlgo::LZ4_INT_TO_BYTE, 100);  // use maximum compression for LZ4 algorithm
      streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * compression);
    }
    else
    {
      defaultCompress = new SingleCompressor(CompAlgo::LZ4_INT_TO_BYTE, 100);  // compression not relevant here
      compress2 = new SingleCompressor(CompAlgo::ZSTD_INT_TO_BYTE, compression - 70);  // use maximum compression for LZ4 algorithm
      streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * (compression - 50));
    }

    streamCompressor->CompressBufferSize(blockSize);

    res = fdsStreamcompressed(myfile, (char*) intP, nrOfRows, 4, streamCompressor, BLOCKSIZE_INT);
    delete defaultCompress;
    delete compress2;
    delete streamCompressor;

    return res;
  }

  if (*nrOfLevels < 32768)  // use 2 bytes per int
  {
    Compressor* defaultCompress = new SingleCompressor(CompAlgo::INT_TO_SHORT, 0);  // compression not relevant here
    Compressor* compress2 = new SingleCompressor(CompAlgo::LZ4_INT_TO_SHORT_SHUF2, 100);  // use maximum compression for LZ4 algorithm
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, compression);
    streamCompressor->CompressBufferSize(blockSize);

    res = fdsStreamcompressed(myfile, (char*) intP, nrOfRows, 4, streamCompressor, BLOCKSIZE_INT);
    delete defaultCompress;
    delete compress2;
    delete streamCompressor;

    return res;
  }

  // use default integer compression with shuffle

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
  StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, compression);
  streamCompressor->CompressBufferSize(blockSize);
  res = fdsStreamcompressed(myfile, (char*) intP, nrOfRows, 4, streamCompressor, BLOCKSIZE_INT);
  delete compress1;
  delete streamCompressor;
  return res;
}


// Parameter 'startRow' is zero based.
SEXP fdsReadFactorVec(ifstream &myfile, SEXP &intVec, unsigned long long blockPos, unsigned int startRow,
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
  SEXP singleColInfo = fdsReadCharVec(myfile, strVec, blockPos + 12, 0, *nrOfLevels, *nrOfLevels);  // get level strings

  // Read level values
  char* values = (char*) INTEGER(intVec);  // output vector
  SEXP intVecInfo = fdsReadColumn(myfile, values, *levelVecPos, startRow, length, size, 4);

  Rf_setAttrib(intVec, Rf_mkString("levels"), strVec);
  Rf_setAttrib(intVec, Rf_mkString("class"), Rf_mkString("factor"));

  return List::create(
    _["singleColInfo"] = singleColInfo,
    _["intVecInfo"] = intVecInfo,
    _["strVec"] = strVec);
}
