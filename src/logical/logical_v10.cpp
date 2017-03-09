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

#include "logical_v10.h"
#include "blockstore_v2.h"

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
SEXP fdsWriteLogicalVec_v10(ofstream &myfile, SEXP &boolVec, unsigned nrOfLogicals, int compression)
{
  int* logicP = LOGICAL(boolVec);  // data pointer
  unsigned int nrOfRows = LENGTH(boolVec);  // vector length

  SEXP res = NULL;

  if (compression == 0)
  {
    FixedRatioCompressor* compressor = new FixedRatioCompressor(CompAlgo::LOGIC64);  // compression level not relevant here
    res = fdsStreamUncompressed_v2(myfile, (char*) logicP, nrOfRows, 4, BLOCKSIZE_LOGICAL, compressor);
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

    res = fdsStreamcompressed_v2(myfile, (char*) logicP, nrOfRows, 4, streamCompressor, BLOCKSIZE_LOGICAL);
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
    res = fdsStreamcompressed_v2(myfile, (char*) logicP, nrOfRows, 4, streamCompressor, BLOCKSIZE_LOGICAL);
    delete compress1;
    delete compress2;
    delete streamCompressor;
  }

  return res;
}


SEXP fdsReadLogicalVec_v10(ifstream &myfile, SEXP &boolVec, unsigned long long blockPos, unsigned int startRow,
  unsigned int length, unsigned int size)
{
  char* values = (char*) LOGICAL(boolVec);  // output vector
  return fdsReadColumn_v2(myfile, values, blockPos, startRow, length, size, 4);
}
