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


// Framework libraries
#include <integer/integer_v8.h>
#include <blockstreamer/blockstreamer_v2.h>
#include <compression/compressor.h>

using namespace std;


void fdsWriteIntVec_v8(ofstream &myfile, int* integerVector, unsigned long long nrOfRows, unsigned int compression,
  std::string annotation, bool hasAnnotation)
{
  int blockSize = 4 * BLOCKSIZE_INT;  // block size in bytes

  if (compression == 0)
  {
    return fdsStreamUncompressed_v2(myfile, reinterpret_cast<char*>(integerVector), nrOfRows, 4, BLOCKSIZE_INT, nullptr, annotation, hasAnnotation);
  }

  if (compression <= 50)  // low compression: linear mix of uncompressed and LZ4_SHUF
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2 * compression);

    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(integerVector), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_SHUF4, 2 * (compression - 50));
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(integerVector), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


void fdsReadIntVec_v8(istream &myfile, int* integerVec, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size, std::string &annotation, bool &hasAnnotation)
{
  return fdsReadColumn_v2(myfile, reinterpret_cast<char*>(integerVec), blockPos, startRow, length, size, 4, annotation, BATCH_SIZE_READ_INT, hasAnnotation);
}
