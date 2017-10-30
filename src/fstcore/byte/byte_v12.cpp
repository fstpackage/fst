/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  BSD 3-Clause (https://opensource.org/licenses/BSD-3-Clause)

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.

  3. Neither the name of the copyright holder nor the names of its
     contributors may be used to endorse or promote products derived from
     this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact the author at :
  - fstlib source repository : https://github.com/fstPackage/fstlib
*/

#include <byte/byte_v12.h>
#include <blockstreamer/blockstreamer_v2.h>

// External libraries
#include <compression/compressor.h>

using namespace std;


void fdsWriteByteVec_v12(ofstream &myfile, char* byteVector, unsigned long long nrOfRows, unsigned int compression, std::string annotation)
{
  int blockSize = BLOCKSIZE_BYTE;  // block size in bytes

  if (compression == 0)
  {
    return fdsStreamUncompressed_v2(myfile, byteVector, nrOfRows, 1, BLOCKSIZE_BYTE, nullptr, annotation);
  }

  if (compression <= 50)  // low compression: linear mix of uncompressed and LZ4_SHUF
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 0);

    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2 * compression);

    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, byteVector, nrOfRows, 1, streamCompressor, BLOCKSIZE_BYTE, annotation);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 0);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD, 0);
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, byteVector, nrOfRows, 1, streamCompressor, BLOCKSIZE_BYTE, annotation);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


void fdsReadByteVec_v12(istream &myfile, char* byteVec, unsigned long long blockPos, unsigned long long startRow, unsigned long long length,
  unsigned long long size)
{
  std::string annotation;

  return fdsReadColumn_v2(myfile, byteVec, blockPos, startRow, length, size, 1, annotation, BATCH_SIZE_READ_BYTE);
}
