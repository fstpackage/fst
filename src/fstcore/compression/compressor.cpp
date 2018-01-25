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

#include <algorithm>
#include <fstream>
#include <cstring>

#include <compression/compressor.h>
#include <compression/compression.h>

#define LZ4_DISABLE_DEPRECATE_WARNINGS  // required for Clang++6.0 compiler error
#include <lz4.h>
#include <zstd.h>


using namespace std;


CompAlgorithm compAlgorithms[NR_OF_ALGORITHMS] = {  // all current and historic compression algorithms
  NoCompression,
  LZ4_C,
  LZ4_C_SHUF4,
  ZSTD_C,
  ZSTD_C_SHUF4,
  LZ4_C_SHUF8,
  ZSTD_C_SHUF8,
  LZ4_LOGIC64_C,
  LOGIC64_C,
  ZSTD_LOGIC64_C,
  LZ4_INT_TO_BYTE_C,
  LZ4_INT_TO_SHORT_SHUF2_C,
  INT_TO_BYTE_C,
  INT_TO_SHORT_C,
  ZSTD_INT_TO_BYTE_C,
  ZSTD_INT_TO_SHORT_SHUF2_C
};


DecompAlgorithm decompAlgorithms[NR_OF_ALGORITHMS] = {  // all current and historic compression algorithms
  NoDecompression,
  LZ4_D,
  LZ4_D_SHUF4,
  ZSTD_D,
  ZSTD_D_SHUF4,
  LZ4_D_SHUF8,
  ZSTD_D_SHUF8,
  LZ4_LOGIC64_D,
  LOGIC64_D,
  ZSTD_LOGIC64_D,
  LZ4_INT_TO_BYTE_D,
  LZ4_INT_TO_SHORT_SHUF2_D,
  INT_TO_BYTE_D,
  INT_TO_SHORT_D,
  ZSTD_INT_TO_BYTE_D,
  ZSTD_INT_TO_SHORT_SHUF2_D
};


CompAlgoType algorithmType[NR_OF_ALGORITHMS] = {  // type of algorithm
  CompAlgoType::UNCOMPRESSED,
  CompAlgoType::LZ4_TYPE,
  CompAlgoType::LZ4_TYPE,
  CompAlgoType::ZSTD_TYPE,
  CompAlgoType::ZSTD_TYPE,
  CompAlgoType::LZ4_TYPE,
  CompAlgoType::ZSTD_TYPE,
  CompAlgoType::LZ4_LOGIC64_TYPE,
  CompAlgoType::LOGIC64_TYPE,
  CompAlgoType::ZSTD_LOGIC64_TYPE,
  CompAlgoType::LZ4_INT_TO_BYTE_TYPE,
  CompAlgoType::LZ4_INT_TO_SHORT_TYPE,
  CompAlgoType::INT_TO_BYTE_TYPE,
  CompAlgoType::INT_TO_SHORT_TYPE,
  CompAlgoType::ZSTD_INT_TO_BYTE_TYPE,
  CompAlgoType::ZSTD_INT_TO_SHORT_TYPE
};


// Source data minimum repeat length
unsigned int fixedRatioSourceRepSize[NR_OF_ALGORITHMS] = {  // all current and historic compression algorithms
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  128,
  0,
  0,
  0,
  32,
  16,
  0,
  0
};


// Target data minimum repeat length
unsigned int fixedRatioTargetRepSize[NR_OF_ALGORITHMS] = {  // all current and historic compression algorithms
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  8,
  0,
  0,
  0,
  8,
  8,
  0,
  0
};


inline int MaxCompressSize(int blockSize, CompAlgoType algoType)
{
  int compBufSize = blockSize;

  switch (algoType)
  {
    case CompAlgoType::LZ4_TYPE:
      compBufSize = LZ4_COMPRESSBOUND(blockSize);  // maximum compressed block size
      break;

    case CompAlgoType::ZSTD_TYPE:
      compBufSize = ZSTD_compressBound(blockSize);  // maximum compressed block size
      break;

    case CompAlgoType::UNCOMPRESSED:
      compBufSize = 0;  // special case: no compressor implemented
      break;

    case CompAlgoType::LOGIC64_TYPE:
    {
      int nrOfLogics = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfLogics - 1) / 32;
      compBufSize = 8 * nrOfLongs;  // safely round upwards
      break;
    }

    case CompAlgoType::LZ4_LOGIC64_TYPE:
    {
      int nrOfLogics = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfLogics - 1) / 32;
      compBufSize = LZ4_COMPRESSBOUND(8 * nrOfLongs);  // 32 logicals are stored in a single 64 bit long
      break;
    }

    case CompAlgoType::ZSTD_LOGIC64_TYPE:
    {
      int nrOfLogics = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfLogics - 1) / 32;
      compBufSize = ZSTD_compressBound(8 * nrOfLongs);  // 32 logicals are stored in a single 64 bit long
      break;
    }

    case CompAlgoType::ZSTD_INT_TO_BYTE_TYPE:
    {
      int nrOfInts = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfInts - 1) / 8;  // 8 integers per long
      compBufSize = ZSTD_compressBound(8 * nrOfLongs);  // 32 logicals are stored in a single 64 bit long
      break;
    }

    case CompAlgoType::LZ4_INT_TO_BYTE_TYPE:
    {
      int nrOfInts = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfInts - 1) / 8;  // 8 integers per long
      compBufSize = LZ4_COMPRESSBOUND(8 * nrOfLongs);  // 32 logicals are stored in a single 64 bit long
      break;
    }

    case CompAlgoType::LZ4_INT_TO_SHORT_TYPE:
    {
      int nrOfInts = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfInts - 1) / 4;  // 4 integers per long
      compBufSize = LZ4_COMPRESSBOUND(8 * nrOfLongs);  // 32 logicals are stored in a single 64 bit long
      break;
    }

    case CompAlgoType::ZSTD_INT_TO_SHORT_TYPE:
    {
      int nrOfInts = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfInts - 1) / 4;  // 4 integers per long
      compBufSize = ZSTD_compressBound(8 * nrOfLongs);  // 32 logicals are stored in a single 64 bit long
      break;
    }


    case CompAlgoType::INT_TO_BYTE_TYPE:
    {
      int nrOfInts = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfInts - 1) / 8;  // 8 integers per long
      compBufSize = 8 * nrOfLongs;
      break;
    }

    case CompAlgoType::INT_TO_SHORT_TYPE:
    {
      int nrOfInts = (blockSize + 3) / 4;  // safely round upwards
      int nrOfLongs = 1 + (nrOfInts - 1) / 4;  // 4 integers per long
      compBufSize = 8 * nrOfLongs;
      break;
    }
  }

  return compBufSize;
}


int Decompressor::Decompress(unsigned int algo, char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  DecompAlgorithm decompAlgorithm = decompAlgorithms[algo];
  return decompAlgorithm(dst, dstCapacity, src, compressedSize);
}


FixedRatioCompressor::FixedRatioCompressor(CompAlgo algo)
{
  this->algo = algo;
  a1 = compAlgorithms[static_cast<int>(algo)];
  repSize = fixedRatioSourceRepSize[static_cast<int>(algo)];
  targetRepSize = fixedRatioTargetRepSize[static_cast<int>(algo)];
}

int FixedRatioCompressor::CompressBufferSize(int maxBlockSize)
{
  int nrOfReps = (maxBlockSize + repSize - 1) / repSize;  // number of minimum rep sizes (rounded upwards)

  return nrOfReps * targetRepSize;  // each rep sized block compresses to targetRepSize bytes
}


int FixedRatioCompressor::Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm)
{
  compAlgorithm = algo;
  return a1(dst, dstCapacity, src, srcSize, 0);  // note that compression level is not relevant here (fixed ratio)
}


SingleCompressor::SingleCompressor(CompAlgo algo1, int compressionLevel)
{
  this->algo1 = algo1;
  this->compLevel = compressionLevel;
  a1 = compAlgorithms[static_cast<int>(algo1)];
}

int SingleCompressor::CompressBufferSize(int maxBlockSize)
{
  return MaxCompressSize(maxBlockSize, algorithmType[static_cast<int>(algo1)]);
}

int SingleCompressor::Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm)
{
  compAlgorithm = algo1;
  return a1(dst, dstCapacity, src, srcSize, compLevel);
}


DualCompressor::DualCompressor(CompAlgo algo1, CompAlgo algo2, int compressionLevel1, int compressionLevel2)
{
  lastCount = 0;
  a1Count = 0.0;
  a1Ratio = 50;
  lastSize1 = 0;
  lastSize2 = 0;

  this->algo1 = algo1;
  this->algo2 = algo2;
  this->compLevel1 = compressionLevel1;
  this->compLevel2 = compressionLevel2;

  a1 = compAlgorithms[static_cast<int>(algo1)];
  a2 = compAlgorithms[static_cast<int>(algo2)];
}

int DualCompressor::CompressBufferSize(int maxBlockSize)
{
  int size1 = MaxCompressSize(maxBlockSize, algorithmType[static_cast<int>(algo1)]);
  int size2 = MaxCompressSize(maxBlockSize, algorithmType[static_cast<int>(algo2)]);
  return max(size1, size2);
}

int DualCompressor::Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm)
{
	int lastCountLocal;
	float a1CountLocal;
	int a1RatioLocal;
	int lastSize1Local;
	int lastSize2Local;

	#pragma omp critical
	{
		lastCountLocal = lastCount;
		a1CountLocal = a1Count;
		a1RatioLocal = a1Ratio;
		lastSize1Local = lastSize1;
		lastSize2Local = lastSize2;
	}

  a1CountLocal += (a1RatioLocal / 100.0);  // check for use of algorithm 1

  if (a1CountLocal > lastCountLocal)
  {
    ++lastCountLocal;
    compAlgorithm = algo1;
    lastSize1Local = a1(dst, dstCapacity, src,  srcSize, compLevel1);

    if (lastSize2Local > lastSize1Local)
    {
      a1RatioLocal = min(95, a1RatioLocal + 5);
    }
    else
    {
      a1RatioLocal = max(5, a1RatioLocal - 5);
    }

	#pragma omp critical (criticalcompression)
	{
		lastCount = lastCountLocal;
		a1Ratio = a1RatioLocal;
		lastSize1 = lastSize1Local;
	}

    return lastSize1Local;
  }

  compAlgorithm = algo2;
  lastSize2Local = a2(dst, dstCapacity, src,  srcSize, compLevel2);

  if (lastSize2Local > lastSize1Local)
  {
    a1RatioLocal = min(95, a1RatioLocal + 5);
  }
  else
  {
    a1RatioLocal = max(5, a1RatioLocal - 5);
  }

	#pragma omp critical (criticalcompression)
	{
		a1Ratio = a1RatioLocal;
		lastSize2 = lastSize2Local;
	}

  return lastSize2Local;
}


StreamLinearCompressor::StreamLinearCompressor(Compressor *compressor, float compressionLevel)
{
  compBufSize = 0;  // remove ?
  compress = compressor;

  compFactor = compressionLevel / 100.0;  // >= 1.00
}

int StreamLinearCompressor::CompressBufferSize()
{
  return compBufSize;  // return buffer size for the compression algorithm
}

int StreamLinearCompressor::CompressBufferSize(unsigned int srcSize)
{
  compBufSize = compress->CompressBufferSize(srcSize);  // return buffer size for the compression algorithm
  return compBufSize;  // return buffer size for the compression algorithm
}

int StreamLinearCompressor::Compress(char* src, unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm, int blockNr)
{
	int delta = static_cast<int>((blockNr + 1) * compFactor) - static_cast<int>(blockNr * compFactor);
	int compSize;

	// Algortihm 1
	if (delta >= 1)
	{
	  compSize = compress->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);
	  return compSize;
	}

	// Uncompressed
	compSize = srcSize;
	compAlgorithm = CompAlgo::UNCOMPRESS;
	memcpy(compBuf, src, srcSize);

	return compSize;
}


StreamSingleCompressor::StreamSingleCompressor(Compressor *compressor)
{
  compress = compressor;
}

int StreamSingleCompressor::CompressBufferSize()
{
  return compBufSize;  // return buffer size for the compression algorithm
}

int StreamSingleCompressor::CompressBufferSize(unsigned int srcSize)
{
  compBufSize = compress->CompressBufferSize(srcSize);  // return buffer size for the compression algorithm
  return compBufSize;  // return buffer size for the compression algorithm
}

int StreamSingleCompressor::Compress(char* src, unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm, int blockNr)
{
  int compSize = compress->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);

  return compSize;
}


StreamCompositeCompressor::StreamCompositeCompressor(Compressor *compressor1, Compressor *compressor2, float compressionLevel)
{
  compress1 = compressor2;
  compress2 = compressor1;

  compFactor = compressionLevel / 100.0;
}

int StreamCompositeCompressor::CompressBufferSize()
{
  return compBufSize;  // return buffer size for the compression algorithm
}

int StreamCompositeCompressor::CompressBufferSize(unsigned int srcSize)
{
  // determine maximum compression buffer size
  compBufSize = max(compress1->CompressBufferSize(srcSize), compress2->CompressBufferSize(srcSize));
  return compBufSize;  // return buffer size for the compression algorithm
}

int StreamCompositeCompressor::Compress(char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm, int blockNr)
{
  int delta = static_cast<int>((blockNr + 1) * compFactor) - static_cast<int>(blockNr * compFactor);
  int compSize;

  // Algortihm 1
  if (delta >= 1)
  {
    compSize = compress1->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);
  }
  else
  {
	compSize = compress2->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm); // Algortihm 2
  }

  return compSize;
}
