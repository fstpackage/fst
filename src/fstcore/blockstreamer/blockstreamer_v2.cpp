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

// System libraries
#include <fstream>
#include <cstring>
#include <iostream>
#include <algorithm>

// Framework libraries
#include <compression/compression.h>
#include <compression/compressor.h>
#include <interface/fstdefines.h>
#include <interface/openmphelper.h>

#include "blockstreamer_v2.h"
#include <memory>

#define BATCH_SIZE_WRITE 25


// Use compile time thread counter for speed
#ifdef _OPENMP
  #include <omp.h>
  #define OMP_GET_THREAD_NUM omp_get_thread_num()
#else
#define OMP_GET_THREAD_NUM 0
#endif

#define COL_META_SIZE 8
#define BLOCK_ALGO_MASK 0xffff000000000000
#define BLOCK_POS_MASK 0x0000ffffffffffff
#define MAX_COMPRESSBOUND_PLUS_META_SIZE 17044


using namespace std;


// Method for writing column data of any type to a ofstream.
void fdsStreamUncompressed_v2(ofstream& myfile, char* vec, unsigned long long vecLength, int elementSize, int blockSizeElems,
  FixedRatioCompressor* fixedRatioCompressor, std::string annotation, bool hasAnnotation)
{
  const unsigned int annotationLength = annotation.length();
  int nrOfBlocks = 1 + (vecLength - 1) / blockSizeElems; // number of compressed / uncompressed blocks
  const int remain = 1 + (vecLength + blockSizeElems - 1) % blockSizeElems; // number of elements in last incomplete block
  int blockSize = blockSizeElems * elementSize;


  if (hasAnnotation)
  {
    unsigned int aLength = annotationLength | 0x80000000;
    myfile.write(reinterpret_cast<char*>(&aLength), 4);

    if (annotationLength > 0)
    {
      myfile.write(annotation.c_str(), annotationLength);
    }
  }
  else // no annotation
  {
    unsigned int aLength = 0;
    myfile.write(reinterpret_cast<char*>(&aLength), 4);
  }

  // nothing to write
  if (vecLength == 0) return;

  // No fixed ratio compressor specified
  if (fixedRatioCompressor == nullptr)
  {
    unsigned int compress[2] = {0, 0};  // set to uncompressed
    myfile.write(reinterpret_cast<char*>(compress), COL_META_SIZE);

    // At most 4 or nrOfBlocks threads and at least 1
    int nrOfThreads = max(1, min(min(4, GetFstThreads()), nrOfBlocks));

    // 1 <= batchSize <= BATCH_SIZE_WRITE
    int batchSize = min(BATCH_SIZE_WRITE, nrOfBlocks / nrOfThreads); // keep thread buffer small

    // batch size in bytes
    unsigned long long totSize = static_cast<unsigned long long>(batchSize) * static_cast<unsigned long long>(blockSize);

    batchSize = max(1, batchSize);  // at least 1 batch

    // each thread has memory available of blockSize * batchSize
    std::unique_ptr<char[]> threadBufferP(new char[nrOfThreads * totSize]);
    char* threadBuffer = threadBufferP.get();

    // number of (partial) batches. Last batch may contain an incomplete last block
    int nrOfBatches = 1 + (nrOfBlocks - 1) / batchSize;

    // Parallel logic starts here

#pragma omp parallel num_threads(nrOfThreads)
    {
#pragma omp for ordered schedule(static, 1)
      for (int batch = 0; batch < nrOfBatches; batch++)
      {
        unsigned long long writeSize = totSize;
        int threadNr = OMP_GET_THREAD_NUM; // use memory buffer specific for this thread

        // last batch
        if (batch == (nrOfBatches - 1))
        {
          writeSize = vecLength * elementSize - batch * totSize;
        }

        // copy to buffer to use core cache more effectively
        char* compBuf = &threadBuffer[threadNr * blockSize * batchSize];

        // the memcpy activates cache buffering
        std::memcpy(compBuf, &vec[batch * totSize], writeSize);

#pragma omp ordered
        {
          myfile.write(compBuf, writeSize);
        }
      }
    }

    return;
  }

  --nrOfBlocks; // set to number of full blocks

  // Use a fixed-ratio compressor

  int remainBlock = remain * elementSize;
  int compressBufSizeRemain = fixedRatioCompressor->CompressBufferSize(remainBlock); // size of block

  char compBuf[MAX_COMPRESSBOUND_PLUS_META_SIZE]; // meta data and compression buffer

  if (nrOfBlocks == 0) // single block
  {
    // char compBuf[compressBufSizeRemain + COL_META_SIZE];  // meta data and compression buffer

    unsigned int* compress = reinterpret_cast<unsigned int*>(compBuf);
    compress[0] = 0;

    CompAlgo compAlgo;
    fixedRatioCompressor->Compress(&compBuf[COL_META_SIZE], compressBufSizeRemain, vec, remainBlock, compAlgo);
    compress[1] = static_cast<unsigned int>(compAlgo); // set fixed-ratio compression algorithm
    myfile.write(compBuf, compressBufSizeRemain + COL_META_SIZE);

    return;
  }

  // More than one block
  int compressBufSize = fixedRatioCompressor->CompressBufferSize(blockSize); // fixed size of a compressed block

  // char compBuf[compressBufSize + COL_META_SIZE];  // meta data and compression buffer

  // First block and metadata

  unsigned int* compress = reinterpret_cast<unsigned int*>(compBuf);
  compress[0] = 0;

  CompAlgo compAlgo;
  fixedRatioCompressor->Compress(&compBuf[COL_META_SIZE], compressBufSize, vec, blockSize, compAlgo);
  compress[1] = static_cast<unsigned int>(compAlgo); // set fixed-ratio compression algorithm
  myfile.write(compBuf, compressBufSize + COL_META_SIZE);

  // Next blocks

  uint64_t blockPos = blockSize;

  for (int block = 1; block != nrOfBlocks; ++block)
  {
    fixedRatioCompressor->Compress(compBuf, compressBufSize, &vec[blockPos], blockSize, compAlgo);
    blockPos += blockSize;
    myfile.write(compBuf, compressBufSize);
  }

  // Last block

  fixedRatioCompressor->Compress(compBuf, compressBufSizeRemain, &vec[blockPos], remainBlock, compAlgo);
  myfile.write(compBuf, compressBufSizeRemain);
}


// header structure
//
//  4                      | unsigned int | maximum compressed size of block 
//  4                      | unsigned int | number of elements in block


// Method for writing column data of any type to a stream.
void fdsStreamcompressed_v2(ofstream& myfile, char* colVec, unsigned long long nrOfRows, int elementSize,
  StreamCompressor* streamCompressor, int blockSizeElems, std::string annotation, bool hasAnnotation)
{
  unsigned int annotationLength = annotation.length();
  int nrOfBlocks = 1 + (nrOfRows - 1) / blockSizeElems; // number of compressed / uncompressed blocks
  int remain = 1 + (nrOfRows + blockSizeElems - 1) % blockSizeElems; // number of elements in last incomplete block
  int blockSize = blockSizeElems * elementSize;

  if (hasAnnotation)
  {
    unsigned int aLength = annotationLength | 0x80000000;
    myfile.write(reinterpret_cast<char*>(&aLength), 4);

    if (annotationLength > 0)
    {
      myfile.write(annotation.c_str(), annotationLength);
    }
  }
  else // no annotation
  {
    unsigned int aLength = 0;
    myfile.write(reinterpret_cast<char*>(&aLength), 4);
  }

  // nothing to write
  if (nrOfRows == 0) return;

  unsigned long long curPos = myfile.tellp();

  // Blocks meta information
  // Aligned at 8 byte boundary
  unsigned int block_index_size = (2 + nrOfBlocks) * 8;
  std::unique_ptr<char[]> blockIndexP(new char[block_index_size]);
  char* blockIndex = blockIndexP.get(); // 1 long file pointer with 2 highest bytes indicating algorithmID

  memset(blockIndex, 0, block_index_size);

  unsigned int* maxCompSize = reinterpret_cast<unsigned int*>(&blockIndex[0]); // maximum uncompressed block length
  unsigned int* blockSizeElements = reinterpret_cast<unsigned int*>(&blockIndex[4]); // number of elements per block

  *blockSizeElements = blockSizeElems;
  *maxCompSize = blockSize; // can be used later for optimization

  // Write block index
  myfile.write(static_cast<char*>(blockIndex), 8 + COL_META_SIZE + nrOfBlocks * 8);
  unsigned long long blockIndexPos = 8 + COL_META_SIZE + nrOfBlocks * 8; // relative to the column data starting position


  // Compress in blocks
  --nrOfBlocks; // Do last block later

  unsigned int maxCompressionSize = 0;
  unsigned long long* blockPosition = reinterpret_cast<unsigned long long*>(&blockIndex[COL_META_SIZE]);

  //////////////////////////////////////////////////////////
  // Parallel logics start here
  //////////////////////////////////////////////////////////

  // total number of blocks: nrOfBlocks + 1
  // last block might be smaller than blockSize

  int nrOfThreads = max(1, min(GetFstThreads(), nrOfBlocks));
  int batchSize = min(BATCH_SIZE_WRITE, nrOfBlocks / nrOfThreads); // keep thread buffer small
  batchSize = max(1, batchSize);

  std::unique_ptr<char[]> threadBufferP(new char[nrOfThreads * MAX_COMPRESSBOUND * batchSize]);
  char* threadBuffer = threadBufferP.get();

  // TODO: possibly memset to zero to avoid valgrind warnings

  int nrOfBatches = nrOfBlocks / batchSize; // number of complete batches with complete blocks

  if (nrOfBatches > 0)
  {
    // Parallel region processes batches with batchSize complete blocks per batch

#pragma omp parallel num_threads(nrOfThreads)
    {
#pragma omp for ordered schedule(static, 1)
      for (int batch = 0; batch < nrOfBatches; batch++)
      {
        unsigned int compSize[BATCH_SIZE_WRITE];
        unsigned int blockAlgorithm[BATCH_SIZE_WRITE];

        int threadNr = OMP_GET_THREAD_NUM; // use memory buffer specific for this thread

        unsigned long long totSize = 0;
        unsigned int localMax = 0;

        for (int offset = 0; offset < batchSize; offset++)
        {
          int block = batch * batchSize + offset;
          CompAlgo compAlgo;
          char* compBuf = &threadBuffer[threadNr * MAX_COMPRESSBOUND * batchSize + totSize];
          unsigned long long vecOffset = static_cast<unsigned long long>(block) * static_cast<unsigned long long>(blockSize);
          compSize[offset] = static_cast<unsigned int>(streamCompressor->Compress(&colVec[vecOffset], blockSize, compBuf, compAlgo, block));
          totSize += static_cast<unsigned long long>(compSize[offset]);
          blockAlgorithm[offset] = static_cast<unsigned int>(compAlgo);
          if (compSize[offset] > localMax) localMax = compSize[offset];
        }


#pragma omp ordered
        {
          for (int offset = 0; offset < batchSize; offset++)
          {
            int block = batch * batchSize + offset;
            blockPosition[block] = blockIndexPos | (static_cast<unsigned long long>(blockAlgorithm[offset]) << 48); // starting position and algorithm in 2 high bytes
            blockIndexPos += compSize[offset]; // compressed block length
          }

          char* compBuf = &threadBuffer[threadNr * MAX_COMPRESSBOUND * batchSize];
          if (localMax > maxCompressionSize) maxCompressionSize = localMax;
          myfile.write(compBuf, totSize);
        }
      }
    }
  }

  //////////////////////////////////////////////////////////
  // Parallel region ends here
  //////////////////////////////////////////////////////////

  // 1 long file pointer and 1 short algorithmID per block
  {
    char* compBuf = threadBuffer;
    unsigned int compSize;
    unsigned int blockAlgorithm;
    unsigned long long totSize = 0;
    CompAlgo compAlgo;

    int fullBlocksRemain = nrOfBlocks - nrOfBatches * batchSize;

    for (int offset = 0; offset < fullBlocksRemain; offset++)
    {
      int block = nrOfBatches * batchSize + offset;

      unsigned long long vecOffset = static_cast<unsigned long long>(block) * static_cast<unsigned long long>(blockSize);
      compSize = static_cast<unsigned int>(streamCompressor->Compress(&colVec[vecOffset], blockSize, &compBuf[totSize], compAlgo, block));
      totSize += compSize;
      blockAlgorithm = static_cast<unsigned int>(compAlgo);
      if (compSize > maxCompressionSize) maxCompressionSize = compSize;

      blockPosition[block] = blockIndexPos | (static_cast<unsigned long long>(blockAlgorithm) << 48); // starting position and algorithm in 2 high bytes
      blockIndexPos += compSize; // compressed block length
    }

    // last (possibly) partial block
    unsigned long long vecOffset = static_cast<unsigned long long>(nrOfBlocks) * static_cast<unsigned long long>(blockSize);
    compSize = static_cast<unsigned int>(streamCompressor->Compress(&colVec[vecOffset], remain * elementSize, &compBuf[totSize], compAlgo, nrOfBlocks));
    totSize += compSize;

    if (compSize > maxCompressionSize) maxCompressionSize = compSize;
    blockAlgorithm = static_cast<unsigned int>(compAlgo);
    blockPosition[nrOfBlocks] = blockIndexPos | (static_cast<unsigned long long>(blockAlgorithm) << 48); // starting position and algorithm in 2 high bytes
    blockIndexPos += compSize; // compressed block length

    myfile.write(compBuf, totSize);
  }

  // Might be usefull in future implementation
  *maxCompSize = maxCompressionSize;

  // Write last block position, note that nrOfBlocks is previously decreased by 1
  blockPosition = reinterpret_cast<unsigned long long*>(&blockIndex[COL_META_SIZE + 8 + nrOfBlocks * 8]);
  *blockPosition = blockIndexPos;

  // Rewrite blockIndex
  myfile.seekp(curPos);
  myfile.write(static_cast<char*>(blockIndex), COL_META_SIZE + 16 + nrOfBlocks * 8);
  myfile.seekp(0, ios_base::end);
}


// Read data compressed with a fixed ratio compressor from a stream
// Note that repSize is assumed to be a multiple of elementSize
inline void fdsReadFixedCompStream_v2(istream& myfile, char* outVec, unsigned long long blockPos,
  unsigned int* meta, unsigned long long startRow, int elementSize, unsigned long long vecLength)
{
  unsigned int compAlgo = meta[1]; // identifier of the fixed ratio compressor
  unsigned int repSize = fixedRatioSourceRepSize[static_cast<int>(compAlgo)]; // in bytes
  unsigned int targetRepSize = fixedRatioTargetRepSize[static_cast<int>(compAlgo)]; // in bytes

  // robustness: test for correct algo here
  if (repSize < 1)
  {
    // throw exception
  }

  // Determine random-access starting point
  unsigned int repSizeElement = repSize / elementSize;
  unsigned int startRep = startRow / repSizeElement;
  unsigned int endRep = (startRow + vecLength - 1) / repSizeElement;

  Decompressor decompressor; // decompressor

  if (startRep > 0)
  {
    myfile.seekg(blockPos + COL_META_SIZE + startRep * targetRepSize); // move to startRep
  }

  unsigned int startRowRep = startRep * repSizeElement;
  unsigned int startOffset = startRow - startRowRep; // rep-block offset in number of elements

  char* outP = outVec; // allow shifting of vector pointer

  // Process partial repetition block
  if (startOffset != 0)
  {
    char repBuf[MAX_TARGET_REP_SIZE]; // rep unit buffer for target
    char buf[MAX_SOURCE_REP_SIZE]; // rep unit buffer for source

    myfile.read(repBuf, targetRepSize); // read single repetition block
    decompressor.Decompress(compAlgo, buf, repSize, repBuf, targetRepSize); // decompress repetition block

    if (startRep == endRep) // finished
    {
      // Skip first startOffset elements
      memcpy(outVec, &buf[elementSize * startOffset], elementSize * vecLength); // data range

      return;
    }

    int length = repSizeElement - startOffset; // remaining elements
    memcpy(outVec, &buf[elementSize * startOffset], elementSize * length); // data range
    outP = &outVec[elementSize * length]; // outVec with correct offset
    ++startRep;
  }

  // Process in large blocks

  // Define prefered block sizes
  unsigned int nrOfRepsPerBlock = (PREF_BLOCK_SIZE / repSize);
  unsigned int nrOfReps = 1 + endRep - startRep; // remaining reps to read
  unsigned int nrOfFullBlocks = (nrOfReps - 1) / nrOfRepsPerBlock; // excluding last (partial) block

  unsigned int blockSize = nrOfRepsPerBlock * repSize; // block size in bytes
  unsigned int targetBlockSize = nrOfRepsPerBlock * targetRepSize; // block size in bytes

  char repBuf[MAX_TARGET_BUFFER]; // maximum size read buffer for PREF_BLOCK_SIZE source
  uint64_t activeBlockPos = 0; // position of active block

  if ((reinterpret_cast<uintptr_t>(outP) % 8) == 0) // aligned pointer
  {
    // Decompress full blocks
    for (unsigned int block = 0; block < nrOfFullBlocks; ++block)
    {
      myfile.read(repBuf, targetBlockSize);
      decompressor.Decompress(compAlgo, &outP[activeBlockPos], blockSize, repBuf, targetBlockSize);
      activeBlockPos += blockSize;
    }
  }
  else
  {
    char alignBuf[PREF_BLOCK_SIZE]; // alignment buffer

    // Decompress full blocks
    for (unsigned int block = 0; block < nrOfFullBlocks; ++block)
    {
      myfile.read(repBuf, targetBlockSize);
      decompressor.Decompress(compAlgo, alignBuf, blockSize, repBuf, targetBlockSize);
      memcpy(&outP[activeBlockPos], alignBuf, blockSize); // move to unaligned output vector
      activeBlockPos += blockSize;
    }
  }

  unsigned int remainReps = nrOfReps - nrOfRepsPerBlock * nrOfFullBlocks; // always > 0 including last rep unit

  // Read last block
  unsigned int lastBlockSize = remainReps * repSize; // block size in bytes
  unsigned int lastTargetBlockSize = remainReps * targetRepSize; // block size in bytes
  myfile.read(repBuf, lastTargetBlockSize);

  // Decompress all but last repetition block fully
  if (lastBlockSize != repSize)
  {
    if ((reinterpret_cast<uintptr_t>(outP) % 8) == 0) // aligned pointer
    {
      decompressor.Decompress(compAlgo, &outP[activeBlockPos], lastBlockSize - repSize, repBuf, lastTargetBlockSize - targetRepSize);
    }
    else
    {
      char alignBuf[PREF_BLOCK_SIZE]; // alignment buffer
      decompressor.Decompress(compAlgo, alignBuf, lastBlockSize - repSize, repBuf, lastTargetBlockSize - targetRepSize);
      memcpy(&outP[activeBlockPos], alignBuf, lastBlockSize - repSize);
    }
  }

  // Last rep unit may be partial
  char buf[MAX_SOURCE_REP_SIZE]; // single rep unit buffer
  unsigned int nrOfElemsLastRep = startRow + vecLength - endRep * repSizeElement;

  decompressor.Decompress(compAlgo, buf, repSize, &repBuf[lastTargetBlockSize - targetRepSize], targetRepSize); // decompress repetition block
  memcpy(&outP[activeBlockPos + lastBlockSize - repSize], buf, elementSize * nrOfElemsLastRep); // skip last elements if required
}

#define UNCOMPRESSED_BLOCKSIZE 262144  // reading in small block is more efficient (probably more efficient L3 caching)

void ProcessBatch(char* outVec, char* blockIndex, unsigned long long blockSize, Decompressor decompressor, unsigned long long outOffset, bool isAlligned,
  unsigned long long blockStart, unsigned long long blockEnd, unsigned long long*& bStart, unsigned long long*& bEnd, char* threadBuf)
{
  unsigned long long totSize = 0;
  for (unsigned long long blockCount = blockStart; blockCount < blockEnd; blockCount++)
  {
    // File offsets and algorithm
    bStart = reinterpret_cast<unsigned long long*>(&blockIndex[8 * blockCount]);
    bEnd = reinterpret_cast<unsigned long long*>(&blockIndex[8 + 8 * blockCount]);
    unsigned short threadAlgo = static_cast<unsigned short>(((*bStart) >> 48) & 0xffff);
    unsigned long long curCompBlockSize = (*bEnd & BLOCK_POS_MASK) - (*bStart & BLOCK_POS_MASK);

    if (threadAlgo == 0) // uncompressed block
    {
      memcpy(&outVec[outOffset + (blockCount - 1) * blockSize], &threadBuf[totSize], blockSize); // copy to misaligned pointer
    }
    else if (isAlligned) // compressed and output vector alligned
    {
      decompressor.Decompress(threadAlgo, &outVec[outOffset + (blockCount - 1) * blockSize], blockSize, &threadBuf[totSize], curCompBlockSize);
    }
    else // misaligned output vector, memcpy to avoid inefficient decompression
    {
      char allignBuf[MAX_SIZE_COMPRESS_BLOCK];
      decompressor.Decompress(threadAlgo, allignBuf, blockSize, &threadBuf[totSize], curCompBlockSize);
      memcpy(&outVec[outOffset + (blockCount - 1) * blockSize], allignBuf, blockSize); // copy to misaligned pointer
    }

    totSize += curCompBlockSize;
  }
}

void fdsReadColumn_v2(istream& myfile, char* outVec, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size, int elementSize, std::string& annotation, int maxbatchSize, bool& hasAnnotation)
{
  myfile.seekg(blockPos);

  unsigned int annotationLength;
  myfile.read(reinterpret_cast<char*>(&annotationLength), 4);

  hasAnnotation = (annotationLength & (1 << 31)) != 0;

  if (hasAnnotation)
  {
    // highest bit is toogle bit for availability
    annotationLength = annotationLength & 0x7fffffff;

    if (annotationLength > 0)
    {
      std::unique_ptr<char[]> annotationBufP(new char[annotationLength]);
      char* annotationBuf = annotationBufP.get();
      myfile.read(annotationBuf, annotationLength);

      annotation += std::string(annotationBuf, annotationLength);
    }
  }

  // there is no data to read
  if (length == 0) return;

  blockPos += 4 + annotationLength;

  // Read header
  unsigned int compress[2];
  myfile.read(reinterpret_cast<char*>(compress), COL_META_SIZE);

  // Data is uncompressed or uses a fixed-ratio compressor (logical)
  if (compress[0] == 0)
  {
    if (compress[1] == 0) // uncompressed data
    {
      // Jump to startRow position
      if (startRow > 0) myfile.seekg(blockPos + elementSize * startRow + COL_META_SIZE);

      uint64_t totBytes = static_cast<uint64_t>(length) * elementSize;

      uint64_t nrOfBlocks = (totBytes - 1) / UNCOMPRESSED_BLOCKSIZE; // all but last block
      uint64_t remainingBytes = totBytes - nrOfBlocks * UNCOMPRESSED_BLOCKSIZE; // last block
      uint64_t curBlockPos = 0;

      // TODO: use multi-threaded logic here

      for (uint64_t block = 0; block != nrOfBlocks; ++block)
      {
        // Read data
        myfile.read(&outVec[curBlockPos], UNCOMPRESSED_BLOCKSIZE);
        curBlockPos += UNCOMPRESSED_BLOCKSIZE;
      }
      myfile.read(&outVec[curBlockPos], remainingBytes);

      return;
    }

    // Stream uses a fixed-ratio compressor

    fdsReadFixedCompStream_v2(myfile, outVec, blockPos, compress, startRow, elementSize, length);

    return;
  }

  // Data is compressed

  // unsigned int* maxCompSize = (unsigned int*) &compress[0];  // 4 algorithms in index
  unsigned int blockSizeElements = compress[1]; // number of elements per block

  // Number of compressed data blocks, the last block can be smaller than blockSizeElements
  unsigned long long nrOfBlocks = 1 + (size - 1) / blockSizeElements;

  // Calculate startRow data block position
  unsigned long long startBlock = startRow / blockSizeElements;
  unsigned long long endBlock = (startRow + length - 1) / blockSizeElements;
  int startOffset = startRow % blockSizeElements;

  if (startBlock > 0)
  {
    myfile.seekg(blockPos + COL_META_SIZE + 8 * startBlock); // move to startBlock meta info
  }

  // Read block index (position pointer and algorithm for each block)
  std::unique_ptr<char[]> blockIndexP(new char[(2 + endBlock - startBlock) * 8]);
  char* blockIndex = blockIndexP.get(); // 1 long file pointer using 2 highest bytes for algorithm

  myfile.read(blockIndex, (2 + endBlock - startBlock) * 8);

  int blockSize = elementSize * blockSizeElements;

  Decompressor decompressor;

  unsigned long long* blockPStart = reinterpret_cast<unsigned long long*>(&blockIndex[0]);
  unsigned long long* blockPEnd = reinterpret_cast<unsigned long long*>(&blockIndex[8]);

  unsigned short algo = static_cast<unsigned short>(((*blockPStart) >> 48) & 0xffff);
  unsigned long long blockPosStart = (*blockPStart) & BLOCK_POS_MASK;
  unsigned long long blockPosEnd = (*blockPEnd) & BLOCK_POS_MASK;
  unsigned long long compSize = blockPosEnd - blockPosStart;

  // unsigned short* algo = (unsigned short*) &blockIndex[8];

  // Process single block and return
  if (startBlock == endBlock) // Read single block and subset result
  {
    char compBuf[MAX_COMPRESSBOUND]; // maximum size needed in worst case scenario compression
    char tmpBuf[MAX_SIZE_COMPRESS_BLOCK]; // temporary buffer

    if (algo == 0) // no compression on this block
    {
      myfile.seekg(blockPos + blockPosStart + elementSize * startOffset); // move to block data position
      myfile.read(static_cast<char*>(outVec), static_cast<uint64_t>(length) * elementSize);

      return;
    }

    // Data is compressed
    unsigned int curSize = blockSizeElements;
    if (startBlock == (nrOfBlocks - 1)) // test for last block
    {
      curSize = 1 + (size + blockSizeElements - 1) % blockSizeElements; // smaller last block size
    }

    myfile.seekg(blockPos + blockPosStart); // move to block data position, not always necessary!
    myfile.read(compBuf, compSize);

    if (length == curSize)
    {
      decompressor.Decompress(algo, outVec, elementSize * length, compBuf, compSize); // direct decompress
    }
    else
    {
      decompressor.Decompress(algo, tmpBuf, elementSize * curSize, compBuf, compSize); // decompress in tmp buffer
      memcpy(outVec, &tmpBuf[elementSize * startOffset], elementSize * length); // data range
    }

    return;
  }

  // Calculations span at least two block

  // First block
  unsigned int subBlockSize = blockSizeElements - startOffset;

  if (algo == 0) // no compression
  {
    myfile.seekg(blockPos + blockPosStart + elementSize * startOffset); // move to block data position
    myfile.read(outVec, elementSize * subBlockSize); // read first block data
  }
  else
  {
    char compBuf[MAX_COMPRESSBOUND]; // maximum size needed in worst case scenario compression
    char tmpBuf[MAX_SIZE_COMPRESS_BLOCK]; // temporary buffer

    myfile.seekg(blockPos + blockPosStart); // move to block data position
    myfile.read(compBuf, compSize);

    if (startOffset == 0) // full block
    {
      decompressor.Decompress(algo, outVec, blockSize, compBuf, compSize);
    }
    else
    {
      decompressor.Decompress(algo, tmpBuf, blockSize, compBuf, compSize);
      memcpy(outVec, &tmpBuf[elementSize * startOffset], elementSize * subBlockSize);
    }
  }

  int remain = (startRow + length) % blockSizeElements; // remaining required items in last block
  if (remain == 0) ++endBlock;

  unsigned long long maxBlock = endBlock - startBlock;
  unsigned long long outOffset = subBlockSize * elementSize; // position in output vector

  // Process middle blocks (if any)

  bool isAlligned = (outOffset % 8) == 0; // test for 8 byte alligned output vector

  maxBlock--; // decrement to get number of full blocks

  const int nrOfThreads = max(1ULL, min(static_cast<unsigned long long>(GetFstThreads()), maxBlock));
  int batchSize = min(static_cast<unsigned long long>(maxbatchSize), maxBlock / nrOfThreads); // keep thread buffer small
  batchSize = max(1, batchSize);

  // TODO: localize threadBuffer in small area
  std::unique_ptr<char[]> threadBufferP(new char[nrOfThreads * MAX_COMPRESSBOUND * batchSize]);
  char* threadBuffer = threadBufferP.get();

  long long nrOfBatches = (maxBlock + batchSize - 1) / batchSize; // number of batches (last one may be smaller)
  long long blockCount = 0;

  //////////////////////////////////////////////////////////
  // Parallel logic starts here
  //////////////////////////////////////////////////////////

#pragma omp parallel num_threads(nrOfThreads) shared(isAlligned,nrOfBatches,batchSize,blockCount)
  {
#pragma omp for schedule(static, 1)
    for (long long blockJob = 0; blockJob < nrOfBatches; blockJob++) // a blockJob is a single unit of work
    {
      int threadNr = OMP_GET_THREAD_NUM; // use memory buffer specific for this thread

      unsigned long long blockStart;
      unsigned long long blockEnd;
      unsigned long long *bStart, *bEnd;
      char* threadBuf = &threadBuffer[threadNr * MAX_COMPRESSBOUND * batchSize]; // MAX_COMPRESSBOUND is adjusted to 16-byte allignment
      int curBatchSize = batchSize;

#pragma omp critical
      {
        blockStart = 1 + blockCount * batchSize;
        bStart = reinterpret_cast<unsigned long long*>(&blockIndex[8 * blockStart]);

        // last batch might have a smaller size
        if (blockCount == (nrOfBatches - 1))
        {
          curBatchSize = batchSize - (nrOfBatches * batchSize % maxBlock);
        }

        blockEnd = blockStart + curBatchSize;

        // determine total length of compressed blocks in batch
        blockCount++;
        bEnd = reinterpret_cast<unsigned long long*>(&blockIndex[8 * blockEnd]);
        unsigned long long curCompSize = (*bEnd & BLOCK_POS_MASK) - (*bStart & BLOCK_POS_MASK);

        myfile.read(threadBuf, curCompSize); // always cache in threadBuf first (non zero copy for uncompressed blocks)
      }

      // Decompress all blocks into output vector

      ProcessBatch(outVec, blockIndex, blockSize, decompressor, outOffset, isAlligned, blockStart, blockEnd, bStart, bEnd, threadBuf);
    }
  }

  //////////////////////////////////////////////////////////
  // Parallel logic ends here
  //////////////////////////////////////////////////////////

  outOffset += maxBlock * blockSize;
  maxBlock++;


  // No last block
  if (remain == 0) // no additional elements required
  {
    return;
  }

  // Process last block

  // Update meta pointers
  blockPStart = reinterpret_cast<unsigned long long*>(&blockIndex[8 * maxBlock]);
  blockPEnd = reinterpret_cast<unsigned long long*>(&blockIndex[8 + 8 * maxBlock]);

  algo = static_cast<unsigned short>(((*blockPStart) >> 48) & 0xffff);
  blockPosStart = (*blockPStart) & BLOCK_POS_MASK;
  blockPosEnd = (*blockPEnd) & BLOCK_POS_MASK;
  compSize = blockPosEnd - blockPosStart;

  int curSize = blockSizeElements; // default block size

  if (algo == 0) // no compression
  {
    myfile.read(&outVec[outOffset], elementSize * remain); // read remaining elements from block
  }
  else
  {
    char compBuf[MAX_COMPRESSBOUND]; // maximum size needed in worst case scenario compression
    char tmpBuf[MAX_SIZE_COMPRESS_BLOCK]; // temporary buffer

    myfile.read(compBuf, compSize);

    if (endBlock == (nrOfBlocks - 1)) // test for last block
    {
      curSize = 1 + (size + blockSizeElements - 1) % blockSizeElements; // smaller last block size
    }

    if (remain == curSize) // full last block
    {
      if ((outOffset % 8) == 0) // outVec pointer is 8-byte aligned
      {
        decompressor.Decompress(algo, &outVec[outOffset], curSize * elementSize, compBuf, compSize);
      }
      else
      {
        decompressor.Decompress(algo, tmpBuf, curSize * elementSize, compBuf, compSize);
        memcpy(&outVec[outOffset], tmpBuf, curSize * elementSize);
      }
    }
    else
    {
      decompressor.Decompress(algo, tmpBuf, curSize * elementSize, compBuf, compSize); // define tmpBuf locally for speed ?
      memcpy(static_cast<char*>(&outVec[outOffset]), tmpBuf, elementSize * remain);
    }
  }
}

