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


#include "integer_v8.h"
#include <blockstreamer_v2.h>

// External libraries
#include <compression.h>
#include <compressor.h>

using namespace std;


void fdsWriteIntVec_v8(ofstream &myfile, int* integerVector, unsigned int nrOfRows, unsigned int compression)
{
  int blockSize = 4 * BLOCKSIZE_INT;  // block size in bytes

  if (compression == 0)
  {
    return fdsStreamUncompressed_v2(myfile, (char*) integerVector, nrOfRows, 4, BLOCKSIZE_INT, nullptr);
  }

  if (compression <= 50)  // low compression: linear mix of uncompressed and LZ4_SHUF
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);

    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2 * compression);

    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, (char*) integerVector, nrOfRows, 4, streamCompressor, BLOCKSIZE_INT);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_SHUF4, 0);
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, (char*) integerVector, nrOfRows, 4, streamCompressor, BLOCKSIZE_INT);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


void fdsReadIntVec_v8(istream &myfile, int* integerVec, unsigned long long blockPos, unsigned int startRow, unsigned int length, unsigned int size)
{
  return fdsReadColumn_v2(myfile, (char*) integerVec, blockPos, startRow, length, size, 4);
}
