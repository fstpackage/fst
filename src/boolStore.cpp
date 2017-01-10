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

#include "boolStore.h"
#include "blockStore.h"

// System libraries
#include <ctime>
#include <ratio>

// External libraries
#include "lz4.h"
#include <compression.h>
#include <compressor.h>

using namespace std;
using namespace Rcpp;
// using namespace std::chrono;

#define BLOCKSIZE_LOGICAL 4096  // number of logicals in default compression block


// Logical vectors are always compressed to fill all available bits (factor 16 compression).
// On top of that, we can compress the resulting bytes with a custom compressor.
SEXP fdsWriteLogicalVec(ofstream &myfile, SEXP &boolVec, unsigned nrOfLogicals, int compression)
{
  int* logicP = LOGICAL(boolVec);  // data pointer
  unsigned int nrOfRows = LENGTH(boolVec);  // vector length

  SEXP res = NULL;

  if (compression == 0)
  {
    FixedRatioCompressor* compressor = new FixedRatioCompressor(CompAlgo::LOGIC64);  // compression level not relevant here
    res = fdsStreamUncompressed(myfile, (char*) logicP, nrOfRows, 4, BLOCKSIZE_LOGICAL, compressor);
    delete compressor;

    return res;
  }

  int blockSize = 4 * BLOCKSIZE_LOGICAL;  // block size in bytes

  if (compression <= 50)  // compress 1 - 50
  {
    Compressor* defaultCompress = new SingleCompressor(CompAlgo::LOGIC64, 0);  // compression not relevant here
    Compressor* compress2 = new SingleCompressor(CompAlgo::LZ4_LOGIC64, 100);  // use maximum compression for LZ4 algorithm
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * compression);
    streamCompressor->CompressBufferSize(blockSize);

    res = fdsStreamcompressed(myfile, (char*) logicP, nrOfRows, 4, streamCompressor, BLOCKSIZE_LOGICAL);
    delete defaultCompress;
    delete compress2;
    delete streamCompressor;

    return res;
  }
  else if (compression <= 100)  // compress 51 - 100
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_LOGIC64, 100);
    Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_LOGIC64, 30 + 7 * (compression - 50) / 5);
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
    streamCompressor->CompressBufferSize(blockSize);
    res = fdsStreamcompressed(myfile, (char*) logicP, nrOfRows, 4, streamCompressor, BLOCKSIZE_LOGICAL);
    delete compress1;
    delete compress2;
    delete streamCompressor;
  }

  return res;
}


SEXP fdsReadLogicalVec(ifstream &myfile, SEXP &boolVec, unsigned long long blockPos, unsigned int startRow,
  unsigned int length, unsigned int size)
{
  char* values = (char*) LOGICAL(boolVec);  // output vector
  return fdsReadColumn(myfile, values, blockPos, startRow, length, size, 4);
}

//
// SEXP fdsReadLogicalVec(ifstream &myfile, SEXP &boolVec, unsigned long long blockPos, unsigned startRow, unsigned length, unsigned size)
// {
//   // Compressed vector is stored in unsigned long long elements with 32 logicals each
//   unsigned startLong = startRow / 32;
//
//   unsigned endRow = startRow + length - 1;
//   unsigned endLong = endRow / 32;
//   unsigned nrOfLongs = 1 + endLong - startLong;
//
//   // Jump to startRow size
//   myfile.seekg(blockPos + 8 * startLong);
//
//   unsigned long long* buf = new unsigned long long[nrOfLongs];
//
//   // Read integer data
//   myfile.read((char*) buf, 8 * nrOfLongs);
//
//   unsigned nrOfLogicals = 1 + endRow - startLong * 32;
//   unsigned offset = startRow % 32;
//
//   LogicDecompr64((char*) LOGICAL(boolVec), buf, nrOfLogicals, offset);
//
//   return List::create(_["1"] = (int) 1);
// }


// [[Rcpp::export]]
SEXP boolToInt(SEXP logical)
{
  int* logicals = LOGICAL(logical);
  int nrOfLogicals = LENGTH(logical);
  int nrOfBytes = nrOfLogicals / 16;
  int* compress = new int[nrOfBytes + 1];

  // Test for sizeof(int) here !!!!!!

  for (int i = 0; i < nrOfBytes; ++i)
  {
    int* logics = &logicals[16 * i];

    compress[i] =
      (* logics        & (1 << 31)) |
      ( (*(logics + 1 ) >> 2 ) & (1 << 29)) |
      ( (*(logics + 2 ) >> 4 ) & (1 << 27)) |
      ( (*(logics + 3 ) >> 6 ) & (1 << 25)) |
      ( (*(logics + 4 ) >> 8 ) & (1 << 23)) |
      ( (*(logics + 5 ) >> 10) & (1 << 21)) |
      ( (*(logics + 6 ) >> 12) & (1 << 19)) |
      ( (*(logics + 7 ) >> 14) & (1 << 17)) |
      ( (*(logics + 8 ) >> 16) & (1 << 15)) |
      ( (*(logics + 9 ) >> 18) & (8192   )) |
      ( (*(logics + 10) >> 20) & (2048   )) |
      ( (*(logics + 11) >> 22) & (512    )) |
      ( (*(logics + 12) >> 24) & (128    )) |
      ( (*(logics + 13) >> 26) & (32     )) |
      ( (*(logics + 14) >> 28) & (8      )) |
      ( (*(logics + 15) >> 30) & (2      )) |
      (* logics       << 30) |
      (*(logics + 1 ) << 28) |
      (*(logics + 2 ) << 26) |
      (*(logics + 3 ) << 24) |
      (*(logics + 4 ) << 22) |
      (*(logics + 5 ) << 20) |
      (*(logics + 6 ) << 18) |
      (*(logics + 7 ) << 16) |
      (*(logics + 8 ) << 14) |
      (*(logics + 9 ) << 12) |
      (*(logics + 10) << 10) |
      (*(logics + 11) << 8 ) |
      (*(logics + 12) << 6 ) |
      (*(logics + 13) << 4 ) |
      (*(logics + 14) << 2 ) |
      (*(logics + 15) & 1 );
  }

  int remain = nrOfLogicals % (nrOfBytes * 16);

  for (int i = 0; i < remain; ++i)
  {
    int* logics = &logicals[16 * nrOfBytes];

    compress[nrOfBytes] |=
      ( (*(logics + i) >> (2 * i) ) & (1 << (31 - 2 * i))) |
      (*(logics + i) << (30 - 2 * i));
  }

  vector<int> compressVec;
  for (int i = 0; i < nrOfBytes + 1; ++i)
  {
    compressVec.push_back(compress[i]);
  }

  return List::create(
    _["nrOfLogicals"] = nrOfLogicals,
    _["nrOfBytes"] = nrOfBytes,
    _["compressVec"] = compressVec);
}


// [[Rcpp::export]]
SEXP intToBool(SEXP ints)
{
  int* compress = INTEGER(ints);
  int nrOfInts = LENGTH(ints);
  int* logicals = new int[nrOfInts * 16];  // create new logical vector

  for (int i = 0; i < nrOfInts; ++i)
  {
    int* logics = &logicals[i * 16];
    int comp = compress[i];

    *(logics     ) = (comp & (1 << 31)) | ((comp >> 30) & 1);
    *(logics + 1 ) = (comp & (1 << 29)) << 2  | ((comp >> 28) & 1);
    *(logics + 2 ) = (comp & (1 << 27)) << 4  | ((comp >> 26) & 1);
    *(logics + 3 ) = (comp & (1 << 25)) << 6  | ((comp >> 24) & 1);
    *(logics + 4 ) = (comp & (1 << 23)) << 8  | ((comp >> 22) & 1);
    *(logics + 5 ) = (comp & (1 << 21)) << 10 | ((comp >> 20) & 1);
    *(logics + 6 ) = (comp & (1 << 19)) << 12 | ((comp >> 18) & 1);
    *(logics + 7 ) = (comp & (1 << 17)) << 14 | ((comp >> 16) & 1);
    *(logics + 8 ) = (comp & (1 << 15)) << 16 | ((comp >> 14) & 1);
    *(logics + 9 ) = (comp & 8192     ) << 18 | ((comp >> 12) & 1);
    *(logics + 10) = (comp & 2048     ) << 20 | ((comp >> 10) & 1);
    *(logics + 11) = (comp & 512      ) << 22 | ((comp >>  8) & 1);
    *(logics + 12) = (comp & 128      ) << 24 | ((comp >>  6) & 1);
    *(logics + 13) = (comp & 32       ) << 26 | ((comp >>  4) & 1);
    *(logics + 14) = (comp & 8        ) << 28 | ((comp >>  2) & 1);
    *(logics + 15) = (comp & 2        ) << 30 | ((comp      ) & 1);
  }


  vector<int> logicalsVec;
  for (int i = 0; i < nrOfInts * 16; ++i)
  {
    logicalsVec.push_back(logicals[i]);
  }

  return List::create(
    _["logicalsVec"] = logicalsVec,
    _["nrOfInts"] = nrOfInts);
}

