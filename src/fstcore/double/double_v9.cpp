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
#include <blockstreamer/blockstreamer_v2.h>
#include <compression/compressor.h>
#include <interface/fstdefines.h>


using namespace std;

void fdsWriteRealVec_v9(ofstream &myfile, double* doubleVector, unsigned long long nrOfRows, unsigned int compression,
  std::string annotation, bool hasAnnotation)
{
  int blockSize = 8 * BLOCKSIZE_REAL;  // block size in bytes

  if (compression == 0)
  {
    return fdsStreamUncompressed_v2(myfile, reinterpret_cast<char*>(doubleVector), nrOfRows, 8, BLOCKSIZE_REAL, nullptr, annotation, hasAnnotation);
  }

  if (compression <= 50)  // low compression: linear mix of uncompressed and LZ4
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 50);
    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2 * compression);
    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(doubleVector), nrOfRows, 8, streamCompressor, BLOCKSIZE_REAL, annotation, hasAnnotation);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, compression);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD, compression - 50);
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(doubleVector), nrOfRows, 8, streamCompressor, BLOCKSIZE_REAL, annotation, hasAnnotation);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


void fdsReadRealVec_v9(istream &myfile, double* doubleVector, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size, std::string &annotation, bool &hasAnnotation)
{
  return fdsReadColumn_v2(myfile, reinterpret_cast<char*>(doubleVector), blockPos, startRow, length, size, 8, annotation,
    BATCH_SIZE_READ_DOUBLE, hasAnnotation);
}
