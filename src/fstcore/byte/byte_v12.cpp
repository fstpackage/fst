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

#include <byte/byte_v12.h>
#include <blockstreamer/blockstreamer_v2.h>

// External libraries
#include <compression/compressor.h>

using namespace std;


void fdsWriteByteVec_v12(ofstream& myfile, char* byteVector, unsigned long long nrOfRows, unsigned int compression,
                         std::string annotation, bool hasAnnotation)
{
  int blockSize = BLOCKSIZE_BYTE; // block size in bytes

  if (compression == 0)
  {
    return fdsStreamUncompressed_v2(myfile, byteVector, nrOfRows, 1, BLOCKSIZE_BYTE, nullptr, annotation, hasAnnotation);
  }

  if (compression <= 50) // low compression: linear mix of uncompressed and LZ4_SHUF
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 0);

    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2 * compression);

    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, byteVector, nrOfRows, 1, streamCompressor, BLOCKSIZE_BYTE, annotation, hasAnnotation);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 0);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD, 0);
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, byteVector, nrOfRows, 1, streamCompressor, BLOCKSIZE_BYTE, annotation, hasAnnotation);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


void fdsReadByteVec_v12(istream& myfile, char* byteVec, unsigned long long blockPos, unsigned long long startRow, unsigned long long length,
                        unsigned long long size)
{
  std::string annotation;
  bool hasAnnotation;

  return fdsReadColumn_v2(myfile, byteVec, blockPos, startRow, length, size, 1, annotation, BATCH_SIZE_READ_BYTE, hasAnnotation);
}
