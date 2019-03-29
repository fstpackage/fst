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

// Standard headers
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <memory>

// Framework headers
#include <interface/istringwriter.h>
#include <factor/factor_v7.h>
#include <blockstreamer/blockstreamer_v2.h>
#include <integer/integer_v8.h>
#include <character/character_v6.h>

#include <compression/compressor.h>

using namespace std;


#define HEADER_SIZE_FACTOR 16
#define VERSION_NUMBER_FACTOR 1

void fdsWriteFactorVec_v7(ofstream &myfile, int* intP, IStringWriter* blockRunner, unsigned long long size, unsigned int compression,
	StringEncoding stringEncoding, std::string annotation, bool hasAnnotation)
{
  unsigned long long blockPos = myfile.tellp();  // offset for factor
  unsigned int nrOfFactorLevels = blockRunner->vecLength;

  // Vector meta data
  char meta[HEADER_SIZE_FACTOR];
  unsigned int* versionNr = reinterpret_cast<unsigned int*>(&meta);
  unsigned int* nrOfLevels = reinterpret_cast<unsigned int*>(&meta[4]);
  unsigned long long* levelVecPos = reinterpret_cast<unsigned long long*>(&meta[8]);

  *versionNr = 0;
  *nrOfLevels = 0;
  *levelVecPos = 0;

  // Use blockrunner to store factor levels if length > 0
  if (nrOfFactorLevels > 0)
  {
	  myfile.write(meta, HEADER_SIZE_FACTOR);  // number of levels
	  *nrOfLevels = nrOfFactorLevels;
	  fdsWriteCharVec_v6(myfile, blockRunner, compression, stringEncoding);   // factor levels

	  // Rewrite meta-data
	  *versionNr = VERSION_NUMBER_FACTOR;
	  *levelVecPos = myfile.tellp();  // offset for level vector

	  myfile.seekp(blockPos);
	  myfile.write(meta, HEADER_SIZE_FACTOR);  // number of levels
	  myfile.seekp(*levelVecPos);  // return to end of file
  }
  else
  {
	  // With zero levels all data values must be zero. Therefore, we only need to
 	  // add the vector length to have enough information.
	  // (see also https://github.com/fstpackage/fst/issues/56)
	  *nrOfLevels = 0;
	  *versionNr = VERSION_NUMBER_FACTOR;
	  *levelVecPos = blockPos + HEADER_SIZE_FACTOR;  // offset for level vector
	  myfile.write(meta, HEADER_SIZE_FACTOR);  // write meta data

	  return;
  }

  // Store factor vector here

  unsigned int nrOfRows = size;  // vector length

  // With zero compression only a fixed width compactor is used (int to byte or int to short)

  // TODO: create multi-threaded code for a fixed ratio compressor

  //if (compression == 0)
  //{
  //  if (*nrOfLevels < 128)
  //  {
  //    FixedRatioCompressor* compressor = new FixedRatioCompressor(CompAlgo::INT_TO_BYTE);  // compression level not relevant here
  //    fdsStreamUncompressed_v2(myfile, (char*) intP, nrOfRows, 4, BLOCKSIZE_INT, compressor, annotation, hasAnnotation);

  //    delete compressor;

  //    return;
  //  }

  //  if (*nrOfLevels < 32768)
  //  {
  //    FixedRatioCompressor* compressor = new FixedRatioCompressor(CompAlgo::INT_TO_SHORT);  // compression level not relevant here
  //    fdsStreamUncompressed_v2(myfile, (char*) intP, nrOfRows, 4, BLOCKSIZE_INT, compressor, annotation, hasAnnotation);
  //    delete compressor;

  //    return;
  //  }

  //  fdsStreamUncompressed_v2(myfile, (char*) intP, nrOfRows, 4, BLOCKSIZE_INT, nullptr, annotation, hasAnnotation);

  //  return;
  //}

  const int blockSize = 4 * BLOCKSIZE_INT;  // block size in bytes

  if (*nrOfLevels < 128)  // use 1 byte per int (Na encoding takes 1 bit)
  {
    if (compression <= 50)
    {
      Compressor* defaultCompress = new SingleCompressor(CompAlgo::INT_TO_BYTE, 0);  // just pack the bytes
      Compressor* compress2 = new SingleCompressor(CompAlgo::LZ4_INT_TO_BYTE, compression);  // maximum compression of 80
      StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * compression);

      streamCompressor->CompressBufferSize(blockSize);

      fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(intP), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

      delete streamCompressor;
      delete compress2;
      delete defaultCompress;

      return;
    }

    Compressor* defaultCompress = new SingleCompressor(CompAlgo::LZ4_INT_TO_BYTE, compression);  // maximum LZ4 compression on smaller vectors
    Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_INT_TO_BYTE, compression - 50);  // maximum compression of 80
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * (compression - 50));

    streamCompressor->CompressBufferSize(blockSize);

    fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(intP), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

    delete streamCompressor;
    delete compress2;
    delete defaultCompress;

    return;
  }

  if (*nrOfLevels < 32768)  // use 2 bytes per int
  {
    if (compression <= 50)
    {
      Compressor* defaultCompress = new SingleCompressor(CompAlgo::LZ4_INT_TO_SHORT_SHUF2, 0);  // just pack the bytes
      Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_INT_TO_SHORT_SHUF2, 0);  // maximum compression of 80
      StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * compression);

      streamCompressor->CompressBufferSize(blockSize);

      fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(intP), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

      delete streamCompressor;
      delete compress2;
      delete defaultCompress;

      return;
    }

    Compressor* defaultCompress = new SingleCompressor(CompAlgo::ZSTD_INT_TO_SHORT_SHUF2, 0);  // maximum LZ4 compression on smaller vectors
    Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_INT_TO_SHORT_SHUF2, compression - 50);  // maximum compression of 80
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2 * (compression - 50));

    streamCompressor->CompressBufferSize(blockSize);

    fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(intP), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

    delete streamCompressor;
    delete compress2;
    delete defaultCompress;

    return;
  }

  // use default integer compression with shuffle

  if (compression <= 50)  // low compression: linear mix of uncompressed and LZ4_SHUF
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2 * compression);

    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(intP), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_SHUF4, 2 * (compression - 50));
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(intP), nrOfRows, 4, streamCompressor, BLOCKSIZE_INT, annotation, hasAnnotation);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


// Parameter 'startRow' is zero based
// Data vector intP is expected to point to a memory block 4 * size bytes long
void fdsReadFactorVec_v7(IFstTable &tableReader, istream &myfile, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size, FstColumnAttribute col_attribute, IColumnFactory* columnFactory, int colSel)
{
  // Jump to factor level
  myfile.seekg(blockPos);

  // Get vector meta data
  char meta[HEADER_SIZE_FACTOR];
  myfile.read(meta, HEADER_SIZE_FACTOR);
  unsigned int* versionNr = (unsigned int*) &meta;

  if (*versionNr > VERSION_NUMBER_FACTOR)
  {
	  throw runtime_error("Incompatible fst file.");
  }

  unsigned int* nrOfLevels = (unsigned int*) &meta[4];
  unsigned long long* levelVecPos = (unsigned long long*) &meta[8];

  // Read level strings

  std::unique_ptr<IFactorColumn> factorColumnP(columnFactory->CreateFactorColumn(length, *nrOfLevels, col_attribute));
  IFactorColumn* factorColumn = factorColumnP.get();

  // add to table
  tableReader.SetFactorColumn(factorColumn, colSel);

  IStringColumn* blockReader = factorColumn->Levels();
  int* intP = factorColumn->LevelData();

  if (*nrOfLevels == 0)
  {
    // All level values must be NA, so we need only the number of levels
    for (unsigned int pos = 0; pos < length; pos++)
    {
      intP[pos] = FST_NA_INT;
    }
  }
  else
  {
    // non-empty level vector
    fdsReadCharVec_v6(myfile, blockReader, blockPos + HEADER_SIZE_FACTOR, 0, *nrOfLevels, *nrOfLevels);  // get level strings

    // Read level values
    std::string annotation;
    bool hasAnnotation;

    fdsReadColumn_v2(myfile, reinterpret_cast<char*>(intP), *levelVecPos, startRow, length, size, 4, annotation, BATCH_SIZE_READ_FACTOR, hasAnnotation);
  }

  return;
}
