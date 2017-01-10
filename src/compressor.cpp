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

#include "compressor.h"
#include "compression.h"

#include "lz4.h"
#include "zstd.h"

#include <Rcpp.h>


using namespace Rcpp;
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
  ZSTD_INT_TO_BYTE_C
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
  ZSTD_INT_TO_BYTE_D
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
  CompAlgoType::ZSTD_INT_TO_BYTE_TYPE
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
  a1 = compAlgorithms[(int) algo];
  repSize = fixedRatioSourceRepSize[(int) algo];
  targetRepSize = fixedRatioTargetRepSize[(int) algo];
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
  a1 = compAlgorithms[(int) algo1];
}

int SingleCompressor::CompressBufferSize(int maxBlockSize)
{
  return MaxCompressSize(maxBlockSize, algorithmType[(int) algo1]);
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

  a1 = compAlgorithms[(int) algo1];
  a2 = compAlgorithms[(int) algo2];
}

int DualCompressor::CompressBufferSize(int maxBlockSize)
{
  int size1 = MaxCompressSize(maxBlockSize, algorithmType[(int) algo1]);
  int size2 = MaxCompressSize(maxBlockSize, algorithmType[(int) algo2]);
  return max(size1, size2);
}

int DualCompressor::Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm)
{
  a1Count += (a1Ratio / 100.0);  // check for use of algorith 1

  if (a1Count > lastCount)
  {
    ++lastCount;
    compAlgorithm = algo1;
    lastSize1 = a1(dst, dstCapacity, src,  srcSize, compLevel1);

    if (lastSize2 > lastSize1)
    {
      a1Ratio = min(95, a1Ratio + 5);
    }
    else
    {
      a1Ratio = max(5, a1Ratio - 5);
    }

    return lastSize1;
  }

  compAlgorithm = algo2;
  lastSize2 = a2(dst, dstCapacity, src,  srcSize, compLevel2);

  if (lastSize2 > lastSize1)
  {
    a1Ratio = min(95, a1Ratio + 5);
  }
  else
  {
    a1Ratio = max(5, a1Ratio - 5);
  }

  return lastSize2;
}


int StreamFixedCompressor::Compress(ofstream &myfile, const char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm)
{
  compAlgorithm = CompAlgo::UNCOMPRESS;
  myfile.write(src, srcSize);
  return srcSize;
}


StreamLinearCompressor::StreamLinearCompressor(Compressor *compressor, float compressionLevel)
{
  compBlockCount = 0;
  curBlock = 0;
  compBufSize = 0;

  compress = compressor;

  if (compressionLevel == 0) compFactor = 1000000;  // no compression
  else compFactor = 100.00001 / compressionLevel;  // >= 1.00

  nextCompBlock = (int) (++compBlockCount * compFactor) - 1;
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

int StreamLinearCompressor::Compress(ofstream &myfile, const char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm)
{
  // Compressed
  if (curBlock >= nextCompBlock)
  {
    int compSize = compress->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);
    myfile.write(compBuf, compSize);
    nextCompBlock = (int) (++compBlockCount * compFactor) - 1;
    ++curBlock;
    return compSize;
  }


  ++curBlock;

  // Uncompressed
  compAlgorithm = CompAlgo::UNCOMPRESS;
  myfile.write(src, srcSize);
  return srcSize;
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

int StreamSingleCompressor::Compress(ofstream &myfile, const char* src, unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm)
{
  int compSize = compress->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);
  myfile.write(compBuf, compSize);
  return compSize;
}

StreamCompositeCompressor::StreamCompositeCompressor(Compressor *compressor1, Compressor *compressor2, float compressionLevel)
{
  compBlockCount = 0;
  curBlock = 0;

  compress1 = compressor2;
  compress2 = compressor1;

  if (compressionLevel == 0) compFactor = 1000000;  // only algorithm 1
  else compFactor = 100.00001 / compressionLevel;  // >= 1.00
  nextCompBlock = (int) (++compBlockCount * compFactor) - 1;
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

int StreamCompositeCompressor::Compress(ofstream &myfile, const char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm)
{
  int compSize;

  // Algortihm 1
  if (curBlock >= nextCompBlock)
  {
    compSize = compress1->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);
    nextCompBlock = (int) (++compBlockCount * compFactor) - 1;
  }
  else
  {
   compSize = compress2->Compress(compBuf, compBufSize, src, srcSize, compAlgorithm);
  }

  myfile.write(compBuf, compSize);
  ++curBlock;
  return compSize;
}
