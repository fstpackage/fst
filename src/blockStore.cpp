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

#include "blockStore.h"

// System libraries
#include <ctime>
#include <ratio>

// External libraries
#include "lz4.h"
#include <compression.h>
#include <compressor.h>

using namespace std;
using namespace Rcpp;

#define COL_META_SIZE 8
#define PREF_BLOCK_SIZE 16384

#define MAX_COMPRESSBOUND_PLUS_META_SIZE 17044


inline unsigned long long CompressBlock(StreamCompressor* streamCompressor, ofstream &myfile, char* vecP, char* compBuf, char* blockIndex,
  int block, unsigned long long blockIndexPos, unsigned int *maxCompSize, int sourceBlockSize)
{
  // 1 long file pointer and 1 short algorithmID per block
  unsigned long long* blockPosition = (unsigned long long*) &blockIndex[COL_META_SIZE + block * 10];
  unsigned short* blockAlgorithm = (unsigned short*) &blockIndex[COL_META_SIZE + 8 + block * 10];
  *blockPosition = blockIndexPos;  // starting position
  
  // Compress block
  CompAlgo compAlgo;
  unsigned int compSize = (unsigned int) streamCompressor->Compress(myfile, vecP, sourceBlockSize, compBuf, compAlgo);
  if (compSize > *maxCompSize) *maxCompSize = compSize;
  *blockAlgorithm = (int) compAlgo;
  
  return compSize;  // compressed block length
}


// Method for writing column data of any type to a ofstream.
SEXP fdsStreamUncompressed(ofstream &myfile, char* vec, unsigned int vecLength, int elementSize, int blockSizeElems,
  FixedRatioCompressor* fixedRatioCompressor)
{
  int nrOfBlocks = 1 + (vecLength - 1) / blockSizeElems;  // number of compressed / uncompressed blocks
  int remain = 1 + (vecLength + blockSizeElems - 1) % blockSizeElems;  // number of elements in last incomplete block
  int blockSize = blockSizeElems * elementSize;
  
  // Write uncompressed vector to disk in blocks
  --nrOfBlocks;  // Do last block later
  
  // No fixed ratio compressor specified
  if (fixedRatioCompressor == NULL )
  {
    unsigned int compress[2] = { 0, 0 };
    myfile.write((char*) compress, COL_META_SIZE);
    
    for (int block = 0; block < nrOfBlocks; ++block)
    {
      myfile.write(&vec[block * blockSize], blockSize);
    }
    myfile.write(&vec[nrOfBlocks * blockSize], remain * elementSize);
    
    return List::create(_["keyNames"] = 1,
      _["vecLength"] = vecLength);
  }
  
  
  // Use a fixed-ratio compressor
  
  int remainBlock = remain * elementSize;
  int compressBufSizeRemain = fixedRatioCompressor->CompressBufferSize(remainBlock);  // size of block
  
  char compBuf[MAX_COMPRESSBOUND_PLUS_META_SIZE];  // meta data and compression buffer
    
  if (nrOfBlocks == 0)  // single block
  {
    // char compBuf[compressBufSizeRemain + COL_META_SIZE];  // meta data and compression buffer
    
    unsigned int *compress = (unsigned int*) compBuf;
    compress[0] = 0;
    
    CompAlgo compAlgo;
    int outSize = fixedRatioCompressor->Compress(&compBuf[COL_META_SIZE], compressBufSizeRemain, vec, remainBlock, compAlgo);
    compress[1] = (unsigned int) compAlgo;  // set fixed-ratio compression algorithm
    myfile.write(compBuf, compressBufSizeRemain + COL_META_SIZE);
    
    return List::create(
      _["compressBufSizeRemain"] = compressBufSizeRemain,
      _["outSize"] = outSize,
      _["remainBlock"] = remainBlock);
  }
  
  // More than one block
  int compressBufSize = fixedRatioCompressor->CompressBufferSize(blockSize);  // fixed size of a compressed block
  
  // char compBuf[compressBufSize + COL_META_SIZE];  // meta data and compression buffer
  
  // First block and metadata
  
  unsigned int *compress = (unsigned int*) compBuf;
  compress[0] = 0;
  
  CompAlgo compAlgo;
  int outSize = fixedRatioCompressor->Compress(&compBuf[COL_META_SIZE], compressBufSize, vec, blockSize, compAlgo);
  compress[1] = (unsigned int) compAlgo;  // set fixed-ratio compression algorithm
  myfile.write(compBuf, compressBufSize + COL_META_SIZE);
  
  // Next blocks
  
  for (int block = 1; block < nrOfBlocks; ++block)
  {
    fixedRatioCompressor->Compress(compBuf, compressBufSize, &vec[block * blockSize], blockSize, compAlgo);
    myfile.write(compBuf, compressBufSize);
  }
  
  // Last block
  
  outSize = fixedRatioCompressor->Compress(compBuf, compressBufSizeRemain, &vec[nrOfBlocks * blockSize], remainBlock, compAlgo);
  myfile.write(compBuf, compressBufSizeRemain);
  
  return List::create(
    _["nrOfBlocks"] = nrOfBlocks,
    _["compressBufSizeRemain"] = compressBufSizeRemain,
    _["compressBufSize"] = compressBufSize,
    _["outSize"] = outSize,
    _["remainBlock"] = remainBlock,
    _["blockSize"] = blockSize);
}


// Method for writing column data of any type to a stream.
SEXP fdsStreamcompressed(ofstream &myfile, char* colVec, unsigned int nrOfRows, int elementSize,
                         StreamCompressor* streamCompressor, int blockSizeElems)
{
  int nrOfBlocks = 1 + (nrOfRows - 1) / blockSizeElems;  // number of compressed / uncompressed blocks
  int remain = 1 + (nrOfRows + blockSizeElems - 1) % blockSizeElems;  // number of elements in last incomplete block
  int blockSize = blockSizeElems * elementSize;
  
  unsigned long long curPos = myfile.tellp();
  
  // Blocks meta information
  char* blockIndex = new char[COL_META_SIZE + 10 + nrOfBlocks * 10];  // 1 long file pointer and 1 short algorithmID per block
  unsigned int* maxCompSize = (unsigned int*) &blockIndex[0];  // maximum uncompressed block length
  unsigned int* blockSizeElements = (unsigned int*) &blockIndex[4];  // number of elements per block
  
  *blockSizeElements = blockSizeElems;
  *maxCompSize = blockSize;  // can be used later for optimization
  
  // Write block index
  myfile.write((char*) blockIndex, 10 + COL_META_SIZE + nrOfBlocks * 10);
  unsigned long long blockIndexPos = 10 + COL_META_SIZE + nrOfBlocks * 10;  // relative to the column data starting position
  

  // Compress in blocks
  
  // int compBufSize =  streamCompressor->CompressBufferSize();  // maximum compressed block size
  
  char compBuf[MAX_COMPRESSBOUND];
  // char compBuf[compBufSize];  // buffer used during compression
  
  --nrOfBlocks;  // Do last block later
  for (int block = 0; block < nrOfBlocks; ++block)
  {
    blockIndexPos += CompressBlock(streamCompressor, myfile, &colVec[block * blockSize], compBuf, blockIndex, block, blockIndexPos, maxCompSize, blockSize);
  }
  blockIndexPos += CompressBlock(streamCompressor, myfile, &colVec[nrOfBlocks * blockSize], compBuf, blockIndex, nrOfBlocks, blockIndexPos, maxCompSize, remain * elementSize);
  
  // Write last block position
  unsigned long long* blockPosition = (unsigned long long*) &blockIndex[COL_META_SIZE + 10 + nrOfBlocks * 10];
  *blockPosition = blockIndexPos;
  
  // Rewrite blockIndex
  myfile.seekp(curPos);
  myfile.write((char*) blockIndex, COL_META_SIZE + 20 + nrOfBlocks * 10);
  myfile.seekp(0, ios_base::end);
  
  delete[] blockIndex;
  
  return List::create(
    _["blockIndexPos"] = (int) blockIndexPos,
    _["nrOfBlocks"] = nrOfBlocks,
    _["curPos"] = (int) curPos);
}


// Read data compressed with a fixed ratio compressor from a stream
// Note that repSize is assumed to be a multiple of elementSize
inline SEXP fdsReadFixedCompStream(ifstream &myfile, char* outVec, unsigned long long blockPos,
                                   unsigned int* meta, unsigned int startRow, int elementSize, unsigned int vecLength)
{
  unsigned int compAlgo = meta[1];  // identifier of the fixed ratio compressor
  unsigned int repSize = fixedRatioSourceRepSize[(int) compAlgo];  // in bytes
  unsigned int targetRepSize = fixedRatioTargetRepSize[(int) compAlgo];  // in bytes
  
  // robustness: test for correct algo here
  if (repSize < 1)
  {
    // throw exception
  }
  
  // Determine random-access starting point
  unsigned int repSizeElement = repSize / elementSize;
  unsigned int startRep = startRow / repSizeElement;
  unsigned int endRep = (startRow + vecLength - 1) / repSizeElement;
  
  Decompressor decompressor;  // decompressor
  
  if (startRep > 0)
  {
    myfile.seekg(blockPos + COL_META_SIZE + startRep * targetRepSize);  // move to startRep
  }
  
  unsigned int startRowRep = startRep * repSizeElement;
  unsigned int startOffset = startRow - startRowRep;  // rep-block offset in number of elements
  
  char* outP = outVec;  // allow shifting of vector pointer
  
  // Process partial repetition block
  if (startOffset != 0)
  {
    char repBuf[MAX_TARGET_REP_SIZE];  // rep unit buffer for target
    char buf[MAX_SOURCE_REP_SIZE];  // rep unit buffer for source
    
    myfile.read(repBuf, targetRepSize);  // read single repetition block
    int resSize = decompressor.Decompress(compAlgo, buf, repSize, repBuf, targetRepSize);  // decompress repetition block
    
    if (startRep == endRep)  // finished
    {
      // Skip first startOffset elements
      memcpy(outVec, &buf[elementSize * startOffset], elementSize * vecLength);  // data range
      
      return List::create(
        _["meta[0]"] = meta[0],
        _["meta[1]"] = meta[1],
        _["repSize"] = repSize,
        _["targetRepSize"] = targetRepSize,
        _["startOffset"] = startOffset,
        _["startRep"] = startRep,
        _["vecLength"] = vecLength,
        _["elementSize"] = elementSize,
        _["repSizeElement"] = repSizeElement,
        _["resSize"] = resSize,
        _["blockPos"] = blockPos);
    }
    
    int length = repSizeElement - startOffset;  // remaining elements
    memcpy(outVec, &buf[elementSize * startOffset], elementSize * length);  // data range
    outP = &outVec[elementSize * length];  // outVec with correct offset
    ++startRep;
  }
  
  // Process in large blocks
  
  // Define prefered block sizes
  unsigned int nrOfRepsPerBlock = (PREF_BLOCK_SIZE / repSize);
  unsigned int nrOfReps = 1 + endRep - startRep;  // remaining reps to read
  unsigned int nrOfFullBlocks = (nrOfReps - 1) / nrOfRepsPerBlock;  // excluding last (partial) block
  
  unsigned int blockSize = nrOfRepsPerBlock * repSize;  // block size in bytes
  unsigned int targetBlockSize = nrOfRepsPerBlock * targetRepSize;  // block size in bytes
  
  char repBuf[MAX_TARGET_BUFFER];  // maximum size read buffer for PREF_BLOCK_SIZE source
  
  
  // Decompress full blocks
  for (unsigned int block = 0; block < nrOfFullBlocks; ++block)
  {
    myfile.read(repBuf, targetBlockSize);
    decompressor.Decompress(compAlgo, &outP[block * blockSize], blockSize, repBuf, targetBlockSize);
  }
  
  unsigned int remainReps = nrOfReps - nrOfRepsPerBlock * nrOfFullBlocks;  // always > 0 including last rep unit
  
  // Read last block
  unsigned int lastBlockSize = remainReps * repSize;  // block size in bytes
  unsigned int lastTargetBlockSize = remainReps * targetRepSize;  // block size in bytes
  myfile.read(repBuf, lastTargetBlockSize);
  
  // Decompress all but last repetition block fully
  if (lastBlockSize != repSize)
  {
    decompressor.Decompress(compAlgo, &outP[nrOfFullBlocks * blockSize], lastBlockSize - repSize, repBuf, lastTargetBlockSize - targetRepSize);
  }
  
  // Last rep unit may be partial
  char buf[MAX_SOURCE_REP_SIZE];  // single rep unit buffer
  unsigned int nrOfElemsLastRep = startRow + vecLength - endRep * repSizeElement;
  
  
  int resSize = decompressor.Decompress(compAlgo, buf, repSize, &repBuf[lastTargetBlockSize - targetRepSize], targetRepSize);  // decompress repetition block
  memcpy(&outP[nrOfFullBlocks * blockSize + lastBlockSize - repSize], buf, elementSize * nrOfElemsLastRep);  // skip last elements if required
  
  return List::create(
    _["nrOfElemsLastRep"] = nrOfElemsLastRep,
    _["remainReps"] = remainReps,
    _["startRep"] = startRep,
    _["nrOfRepsPerBlock"] = nrOfRepsPerBlock,
    _["endRep"] = endRep,
    _["repSize"] = repSize,
    _["resSize"] = resSize,
    _["nrOfReps"] = nrOfReps,
    _["nrOfFullBlocks"] = nrOfFullBlocks,
    _["startRow"] = startRow,
    _["lastTargetBlockSize"] = lastTargetBlockSize);
}


SEXP fdsReadColumn(ifstream &myfile, char* outVec, unsigned long long blockPos, unsigned startRow, unsigned length, unsigned size, int elementSize)
{
  // Read header
  unsigned int compress[2];
  myfile.seekg(blockPos);
  myfile.read((char*) compress, COL_META_SIZE);
  
  // Data is uncompressed or uses a fixed-ratio compressor (logical)
  if (compress[0] == 0)
  {
    if (compress[1] == 0)  // uncompressed data
    {
      // Jump to startRow position
      if (startRow > 0) myfile.seekg(blockPos + elementSize * startRow + COL_META_SIZE);
      
      // Read data
      myfile.read((char*) outVec, elementSize * length);
      
      return List::create(_["1"] = (int) 1);
    }
    
    // Stream uses a fixed-ratio compressor
    
    SEXP res = fdsReadFixedCompStream(myfile, outVec, blockPos, compress, startRow, elementSize, length);
    return List::create(
      _["res"]  = res);
  }
  
  // Data is compressed
  
  // unsigned int* maxCompSize = (unsigned int*) &compress[0];  // 4 algorithms in index
  unsigned int blockSizeElements = compress[1];  // number of elements per block
  
  // Number of compressed data blocks, the last block can be smaller than blockSizeElements
  int nrOfBlocks = 1 + (size - 1) / blockSizeElements;
  
  // Calculate startRow data block position
  int startBlock = startRow / blockSizeElements;
  int endBlock = (startRow + length - 1) / blockSizeElements;
  int startOffset = startRow % blockSizeElements;
  
  if (startBlock > 0)
  {
    myfile.seekg(blockPos + COL_META_SIZE + 10 * startBlock);  // move to startBlock meta info
  }
  
  // Read block index (position pointer and algorithm for each block)
  char* blockIndex = new char[(2 + endBlock - startBlock) * 10];  // 1 long file pointer and 1 short algorithmID per block
  myfile.read(blockIndex, (2 + endBlock - startBlock) * 10);
  
  int blockSize = elementSize * blockSizeElements;
  
  // char compBuf[*maxCompSize];  // read buffer
  // char tmpBuf[blockSize];  // temporary buffer
  char compBuf[MAX_COMPRESSBOUND];  // maximum size needed in worst case scenario compression
  char tmpBuf[MAX_SIZE_COMPRESS_BLOCK];  // temporary buffer
  
  Decompressor decompressor;
  
  unsigned long long* blockPStart = (unsigned long long*) &blockIndex[0];
  unsigned long long* blockPEnd = (unsigned long long*) &blockIndex[10];
  unsigned long long compSize = *blockPEnd - *blockPStart;
  unsigned short* algo = (unsigned short*) &blockIndex[8];
  
  // Process single block and return
  if (startBlock == endBlock)  // Read single block and subset result
  {
    if (*algo == 0)  // no compression on this block
    {
      myfile.seekg(blockPos + *blockPStart + elementSize * startOffset);  // move to block data position
      myfile.read((char*) outVec, length * elementSize);
      
      delete[] blockIndex;
      return List::create(
        _["startBlock"] = startBlock,
        _["endBlock"] = endBlock,
        _["startOffset"] = startOffset,
        _["blockPStart"] = (int) (blockPos + *blockPStart),
        _["blockPEnd"] = (int) (blockPos + *blockPEnd),
        _["blockPos"] = (int) blockPos,
        _["algo"] = (int) *algo);
    }
    
    // Data is compressed
    unsigned int curSize = blockSizeElements;
    if (startBlock == (nrOfBlocks - 1))  // test for last block
    {
      curSize = 1 + (size + blockSizeElements - 1) % blockSizeElements;  // smaller last block size
    }
    
    myfile.seekg(blockPos + *blockPStart);  // move to block data position
    myfile.read(compBuf, compSize);
    
    if (length == curSize)
    {
      decompressor.Decompress(*algo, outVec, elementSize * length, compBuf, compSize);  // direct decompress
    }
    else
    {
      decompressor.Decompress(*algo, tmpBuf, elementSize * curSize, compBuf, compSize);  // decompress in tmp buffer
      memcpy(outVec, &tmpBuf[elementSize * startOffset], elementSize * length);  // data range
    }
    
    delete[] blockIndex;
    return List::create(
      _["curSize"] = curSize,
      _["startBlock"] = startBlock,
      _["endBlock"] = endBlock,
      _["startOffset"] = startOffset,
      _["blockPStart"] = (int) (blockPos + *blockPStart),
      _["blockPEnd"] = (int) (blockPos + *blockPEnd),
      _["blockPos"] = (int) blockPos,
      _["algo"] = (int) *algo);
  }
  
  // Calculations span at least two block
  
  // First block
  int subBlockSize = blockSizeElements - startOffset;
  
  if (*algo == 0)  // no compression
  {
    myfile.seekg(blockPos + *blockPStart + elementSize * startOffset);  // move to block data position
    myfile.read(outVec, elementSize * subBlockSize);  // read first block data
  } else
  {
    myfile.seekg(blockPos + *blockPStart);  // move to block data position
    myfile.read(compBuf, compSize);
    
    if (startOffset == 0)  // full block
    {
      decompressor.Decompress(*algo, outVec, blockSize, compBuf, compSize);
    }
    else
    {
      decompressor.Decompress(*algo, tmpBuf, blockSize, compBuf, compSize);
      memcpy(outVec, &tmpBuf[elementSize * startOffset], elementSize * subBlockSize);
    }
  }
  
  int remain = (startRow + length) % blockSizeElements;  // remaining required items in last block
  if (remain == 0) ++endBlock;
  
  int maxBlock = endBlock - startBlock;
  int outOffset = subBlockSize * elementSize;  // position in output vector
  
  // Process middle blocks (if any)
  for (int blockCount = 1; blockCount < maxBlock; ++blockCount)
  {
    // Update meta pointers
    blockPStart = blockPEnd;
    blockPEnd = (unsigned long long*) &blockIndex[10 + 10 * blockCount];
    compSize = *blockPEnd - *blockPStart;
    algo = (unsigned short*) &blockIndex[8 + 10 * blockCount];
    
    if (*algo == 0)  // no compression
    {
      myfile.read(&outVec[outOffset], blockSize);  // read first block data
    } else
    {
      myfile.read(compBuf, compSize);
      decompressor.Decompress(*algo, &outVec[outOffset], blockSize, compBuf, compSize);
    }
    
    outOffset += blockSize;  // update position in output vector
  }
  
  // No last block
  if (remain == 0)  // no additional elements required
  {
    delete[] blockIndex;
    return List::create(
      _["Remain0"] = true,
      _["endBlock"] = endBlock,
      _["compSize"] = compSize,
      _["algo"] = (int) (*algo),
      _["maxBlock"] = maxBlock);
  }
  
  // Process last block
  
  // Update meta pointers
  blockPStart = blockPEnd;
  blockPEnd = (unsigned long long*) &blockIndex[10 + 10 * maxBlock];
  compSize = *blockPEnd - *blockPStart;
  algo = (unsigned short*) &blockIndex[8 + 10 * maxBlock];
  
  int curSize = blockSizeElements;  // default block size
  
  if (*algo == 0)  // no compression
  {
    myfile.read(&outVec[outOffset], elementSize * remain);  // read remaining elements from block
  } else
  {
    myfile.read(compBuf, compSize);
    
    if (endBlock == (nrOfBlocks - 1))  // test for last block
    {
      curSize = 1 + (size + blockSizeElements - 1) % blockSizeElements;  // smaller last block size
    }
    
    if (remain == curSize)  // full last block
    {
      decompressor.Decompress(*algo, &outVec[outOffset], curSize * elementSize, compBuf, compSize);
    }
    else
    {
      decompressor.Decompress(*algo, tmpBuf, curSize * elementSize, compBuf, compSize);  // define tmpBuf locally for speed ?
      memcpy((char*) &outVec[outOffset], tmpBuf, elementSize * remain);
    }
  }
  
  delete[] blockIndex;
  return List::create(
    _["compSize"] = compSize,
    _["algo"] = (int) (*algo),
    _["maxBlock"] = maxBlock,
    _["remain"] = remain,
    _["curSize"] = curSize,
    _["blockPStart"] = (int) *blockPStart,
    _["blockPEnd"] = (int) *blockPEnd);
}

