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

#include "character/character_v6.h"
#include "interface/istringwriter.h"
#include "interface/fstdefines.h"
#include <compression/compressor.h>

#include <fstream>
#include <memory>
#include <cstring>  // memset


// #include <boost/unordered_map.hpp>

using namespace std;


inline unsigned int StoreCharBlock_v6(ofstream& myfile, IStringWriter* blockRunner, unsigned long long startCount, unsigned long long endCount)
{
  blockRunner->SetBuffersFromVec(startCount, endCount);

  unsigned int nrOfElements = endCount - startCount; // the string at position endCount is not included
  unsigned int nrOfNAInts = 1 + nrOfElements / 32; // add 1 bit for NA present flag

  myfile.write(reinterpret_cast<char*>(blockRunner->strSizes), nrOfElements * 4); // write string lengths
  myfile.write(reinterpret_cast<char*>(blockRunner->naInts), nrOfNAInts * 4); // write string lengths

  unsigned int totSize = blockRunner->bufSize;

  myfile.write(blockRunner->activeBuf, totSize);

  return totSize + (nrOfElements + nrOfNAInts) * 4;
}


inline unsigned int storeCharBlockCompressed_v6(ofstream& myfile, IStringWriter* blockRunner, unsigned int startCount,
  unsigned int endCount, StreamCompressor* intCompressor, StreamCompressor* charCompressor, unsigned short int& algoInt,
  unsigned short int& algoChar, int& intBufSize, int blockNr)
{
  // Determine string lengths
  unsigned int nrOfElements = endCount - startCount; // the string at position endCount is not included
  unsigned int nrOfNAInts = 1 + nrOfElements / 32; // add 1 bit for NA present flag


  // Compress string size vector
  unsigned int strSizesBufLength = nrOfElements * 4;

  // 1) Use stack buffer here !!!!!!
  // 2) compress to 1 or 2 bytes if possible with strSizesBufLength
  int bufSize = intCompressor->CompressBufferSize(strSizesBufLength); // 1 integer per string

  std::unique_ptr<char[]> intBufP(new char[bufSize]);
  char* intBuf = intBufP.get();

  CompAlgo compAlgorithm;
  intBufSize = intCompressor->Compress(reinterpret_cast<char*>(blockRunner->strSizes), strSizesBufLength, intBuf, compAlgorithm, blockNr);
  myfile.write(intBuf, intBufSize);

  //intCompressor->WriteBlock(myfile, (char*)(stringWriter->strSizes), intBuf);
  algoInt = static_cast<unsigned short int>(compAlgorithm); // store selected algorithm

  // Write NA bits uncompressed (add compression later ?)
  myfile.write(reinterpret_cast<char*>(blockRunner->naInts), nrOfNAInts * 4); // write string lengths

  unsigned int totSize = blockRunner->bufSize;

  int compBufSize = charCompressor->CompressBufferSize(totSize);

  std::unique_ptr<char[]> compBufP(new char[compBufSize]);
  char* compBuf = compBufP.get();

  // Compress buffer
  int resSize = charCompressor->Compress(blockRunner->activeBuf, totSize, compBuf, compAlgorithm, blockNr);
  myfile.write(compBuf, resSize);

  algoChar = static_cast<unsigned short int>(compAlgorithm); // store selected algorithm

  return nrOfNAInts * 4 + resSize + intBufSize;
}


void fdsWriteCharVec_v6(ofstream& myfile, IStringWriter* stringWriter, int compression, StringEncoding stringEncoding)
{
  uint64_t vecLength = stringWriter->vecLength; // expected to be larger than zero

  // nothing to write
  if (vecLength == 0) return;

  uint64_t curPos = myfile.tellp();
  uint64_t nrOfBlocks = (vecLength - 1) / BLOCKSIZE_CHAR; // number of blocks minus 1

  if (compression == 0)
  {
    uint32_t metaSize = CHAR_HEADER_SIZE + (nrOfBlocks + 1) * 8;

    // first CHAR_HEADER_SIZE bytes store compression setting and block size
    std::unique_ptr<char[]> metaP(new char[metaSize]);
    char* meta = metaP.get();

    // clear memory for safety
    memset(meta, 0, metaSize);

    // Set column header
    uint32_t* isCompressed = reinterpret_cast<uint32_t*>(meta);
    uint32_t* blockSizeChar = reinterpret_cast<uint32_t*>(&meta[4]);
    *blockSizeChar = BLOCKSIZE_CHAR; // check why 2047 and not 2048
    *isCompressed = stringEncoding << 1;

    myfile.write(meta, metaSize); // write block offset index

    uint64_t* blockPos = reinterpret_cast<uint64_t*>(&meta[CHAR_HEADER_SIZE]);
    uint64_t fullSize = metaSize;

    for (uint64_t block = 0; block < nrOfBlocks; ++block)
    {
      uint32_t totSize = StoreCharBlock_v6(myfile, stringWriter, block * BLOCKSIZE_CHAR, (block + 1) * BLOCKSIZE_CHAR);
      fullSize += totSize;
      blockPos[block] = fullSize;
    }

    uint32_t totSize = StoreCharBlock_v6(myfile, stringWriter, nrOfBlocks * BLOCKSIZE_CHAR, vecLength);
    fullSize += totSize;
    blockPos[nrOfBlocks] = fullSize;

    myfile.seekp(curPos + CHAR_HEADER_SIZE);
    myfile.write(reinterpret_cast<char*>(blockPos), (nrOfBlocks + 1) * 8); // additional zero for index convenience
    myfile.seekp(curPos + fullSize); // back to end of file

    return;
  }


  // Use compression

  uint32_t metaSize = CHAR_HEADER_SIZE + (nrOfBlocks + 1) * CHAR_INDEX_SIZE; // 1 long and 2 unsigned int per block

  std::unique_ptr<char[]> metaP(new char[metaSize]);
  char* meta = metaP.get();

  // clear memory for safety
  memset(meta, 0, metaSize);

  // Set column header
  uint32_t* isCompressed = reinterpret_cast<uint32_t*>(meta);
  uint32_t* blockSizeChar = reinterpret_cast<uint32_t*>(&meta[4]);
  *blockSizeChar = BLOCKSIZE_CHAR;
  *isCompressed = (stringEncoding << 1) | 1; // set compression flag

  myfile.write(meta, metaSize); // write block offset and algorithm index

  char* blockP = &meta[CHAR_HEADER_SIZE];

  unsigned long long fullSize = metaSize;

  // Compressors
  Compressor* compressInt;
  Compressor* compressInt2 = nullptr;
  StreamCompressor* streamCompressInt;
  Compressor* compressChar;
  Compressor* compressChar2 = nullptr;
  StreamCompressor* streamCompressChar;

  // Compression settings
  if (compression <= 50)
  {
    // Integer vector compressor
    compressInt = new SingleCompressor(LZ4_SHUF4, 0);
    streamCompressInt = new StreamLinearCompressor(compressInt, 2 * compression);

    // Character vector compressor
    compressChar = new SingleCompressor(LZ4, 20);
    streamCompressChar = new StreamLinearCompressor(compressChar, 2 * compression); // unknown blockSize
  }
  else // 51 - 100
  {
    // Integer vector compressor
    compressInt = new SingleCompressor(LZ4_SHUF4, 0);
    compressInt2 = new SingleCompressor(ZSTD_SHUF4, 0);
    streamCompressInt = new StreamCompositeCompressor(compressInt, compressInt2, 2 * (compression - 50));

    // Character vector compressor
    compressChar = new SingleCompressor(LZ4, 20);
    compressChar2 = new SingleCompressor(ZSTD, 20);
    streamCompressChar = new StreamCompositeCompressor(compressChar, compressChar2, 2 * (compression - 50));
  }

  for (unsigned long long block = 0; block < nrOfBlocks; ++block)
  {
    unsigned long long* blockPos = reinterpret_cast<unsigned long long*>(blockP);
    unsigned short int* algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
    unsigned short int* algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
    int* intBufSize = reinterpret_cast<int*>(blockP + 12);

    stringWriter->SetBuffersFromVec(block * BLOCKSIZE_CHAR, (block + 1) * BLOCKSIZE_CHAR);
    unsigned long long totSize = storeCharBlockCompressed_v6(myfile, stringWriter, block * BLOCKSIZE_CHAR,
      (block + 1) * BLOCKSIZE_CHAR, streamCompressInt, streamCompressChar, *algoInt, *algoChar, *intBufSize, block);

    fullSize += totSize;
    *blockPos = fullSize;
    blockP += CHAR_INDEX_SIZE; // advance one block index entry
  }

  unsigned long long* blockPos = reinterpret_cast<unsigned long long*>(blockP);
  unsigned short int* algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  unsigned short int* algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  int* intBufSize = reinterpret_cast<int*>(blockP + 12);

  stringWriter->SetBuffersFromVec(nrOfBlocks * BLOCKSIZE_CHAR, vecLength);
  unsigned long long totSize = storeCharBlockCompressed_v6(myfile, stringWriter, nrOfBlocks * BLOCKSIZE_CHAR,
    vecLength, streamCompressInt, streamCompressChar, *algoInt, *algoChar, *intBufSize, nrOfBlocks);

  fullSize += totSize;
  *blockPos = fullSize;

  delete streamCompressInt;
  delete streamCompressChar;
  delete compressInt;
  delete compressInt2;
  delete compressChar;
  delete compressChar2;

  myfile.seekp(curPos + CHAR_HEADER_SIZE);
  myfile.write(static_cast<char*>(&meta[CHAR_HEADER_SIZE]), (nrOfBlocks + 1) * CHAR_INDEX_SIZE); // additional zero for index convenience
  myfile.seekp(0, ios_base::end);
}


inline void ReadDataBlock_v6(istream& myfile, IStringColumn* blockReader, unsigned long long blockSize, unsigned long long nrOfElements,
                             unsigned long long startElem, unsigned long long endElem, unsigned long long vecOffset)
{
  unsigned long long nrOfNAInts = 1 + nrOfElements / 32; // last bit is NA flag
  unsigned long long totElements = nrOfElements + nrOfNAInts;

  std::unique_ptr<unsigned int[]> sizeMetaP(new unsigned int[totElements]);
  unsigned int* sizeMeta = sizeMetaP.get();

  myfile.read(reinterpret_cast<char*>(sizeMeta), totElements * 4); // read cumulative string lengths and NA bits

  unsigned int charDataSize = blockSize - totElements * 4;

  std::unique_ptr<char[]> bufP(new char[charDataSize]);
  char* buf = bufP.get();

  myfile.read(buf, charDataSize); // read string lengths

  blockReader->BufferToVec(nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);
}


inline void ReadDataBlockCompressed_v6(istream& myfile, IStringColumn* blockReader, unsigned long long blockSize, unsigned long long nrOfElements,
  unsigned long long startElem, unsigned long long endElem, unsigned long long vecOffset,
  unsigned int intBlockSize, Decompressor& decompressor, unsigned short int& algoInt, unsigned short int& algoChar)
{
  unsigned long long nrOfNAInts = 1 + nrOfElements / 32; // NA metadata including overall NA bit
  unsigned long long totElements = nrOfElements + nrOfNAInts;

  std::unique_ptr<unsigned int[]> sizeMetaP(new unsigned int[totElements]);
  unsigned int* sizeMeta = sizeMetaP.get();

  // Read and uncompress str sizes data
  if (algoInt == 0) // uncompressed
  {
    myfile.read(reinterpret_cast<char*>(sizeMeta), totElements * 4); // read cumulative string lengths
  }
  else
  {
    unsigned int intBufSize = intBlockSize;

    std::unique_ptr<char[]> strSizeBufP(new char[intBufSize]);
    char* strSizeBuf = strSizeBufP.get();

    myfile.read(strSizeBuf, intBufSize);
    myfile.read(reinterpret_cast<char*>(&sizeMeta[nrOfElements]), nrOfNAInts * 4); // read cumulative string lengths

    // Decompress size but not NA metadata (which is currently uncompressed)

    decompressor.Decompress(algoInt, reinterpret_cast<char*>(sizeMeta), nrOfElements * 4, strSizeBuf, intBlockSize);
  }

  unsigned int charDataSizeUncompressed = sizeMeta[nrOfElements - 1];

  // Read and uncompress string vector data, use stack if possible here !!!!!
  unsigned int charDataSize = blockSize - intBlockSize - nrOfNAInts * 4;

  std::unique_ptr<char[]> bufP(new char[charDataSizeUncompressed]);
  char* buf = bufP.get();

  if (algoChar == 0)
  {
    myfile.read(buf, charDataSize); // read string lengths
  }
  else
  {
    std::unique_ptr<char[]> bufCompressedP(new char[charDataSize]);
    char* bufCompressed = bufCompressedP.get();

    myfile.read(bufCompressed, charDataSize); // read string lengths
    decompressor.Decompress(algoChar, buf, charDataSizeUncompressed, bufCompressed, charDataSize);
  }

  blockReader->BufferToVec(nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);
}


void fdsReadCharVec_v6(istream& myfile, IStringColumn* blockReader, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long vecLength, unsigned long long size)
{
  // nothing to read
  if (vecLength == 0) return;

  // Jump to startRow size
  myfile.seekg(blockPos);

  // Read algorithm type and block size
  unsigned int meta[2];
  myfile.read(reinterpret_cast<char*>(meta), CHAR_HEADER_SIZE);

  unsigned int compression = meta[0] & 1; // maximum 8 encodings
  StringEncoding stringEncoding = static_cast<StringEncoding>(meta[0] >> 1 & 7); // at maximum 8 encodings

  unsigned long long blockSizeChar = static_cast<unsigned long long>(meta[1]);
  unsigned long long totNrOfBlocks = (size - 1) / blockSizeChar; // total number of blocks minus 1
  unsigned long long startBlock = startRow / blockSizeChar;
  unsigned long long startOffset = startRow - (startBlock * blockSizeChar);
  unsigned long long endBlock = (startRow + vecLength - 1) / blockSizeChar;
  unsigned long long endOffset = (startRow + vecLength - 1) - endBlock * blockSizeChar;
  unsigned long long nrOfBlocks = 1 + endBlock - startBlock; // total number of blocks to read

  // Create result vector
  // blockReader->AllocateVec(vecLength);
  blockReader->SetEncoding(stringEncoding);

  // Vector data is uncompressed
  if (compression == 0)
  {
    std::unique_ptr<unsigned long long[]> blockOffsetP(new unsigned long long[1 + nrOfBlocks]);
    unsigned long long* blockOffset = blockOffsetP.get(); // block positions

    if (startBlock > 0) // include previous block offset
    {
      myfile.seekg(blockPos + CHAR_HEADER_SIZE + (startBlock - 1) * 8); // jump to correct block index
      myfile.read(reinterpret_cast<char*>(blockOffset), (1 + nrOfBlocks) * 8);
    }
    else
    {
      blockOffset[0] = CHAR_HEADER_SIZE + (totNrOfBlocks + 1) * 8;
      myfile.read(reinterpret_cast<char*>(&blockOffset[1]), nrOfBlocks * 8);
    }


    // Navigate to first selected data block
    unsigned long long offset = blockOffset[0];
    myfile.seekg(blockPos + offset);

    unsigned long long endElem = blockSizeChar - 1;
    unsigned long long nrOfElements = blockSizeChar;

    if (startBlock == endBlock) // subset start and end of block
    {
      endElem = endOffset;
      if (endBlock == totNrOfBlocks)
      {
        nrOfElements = size - totNrOfBlocks * blockSizeChar; // last block can have less elements
      }
    }

    // Read first block with offset
    unsigned long long blockSize = blockOffset[1] - offset; // size of data block

    ReadDataBlock_v6(myfile, blockReader, blockSize, nrOfElements, startOffset, endElem, 0);

    if (startBlock == endBlock) // subset start and end of block
    {
      return;
    }

    offset = blockOffset[1];
    unsigned long long vecPos = blockSizeChar - startOffset;

    if (endBlock == totNrOfBlocks)
    {
      nrOfElements = size - totNrOfBlocks * blockSizeChar; // last block can have less elements
    }

    --nrOfBlocks; // iterate full blocks
    for (unsigned long long block = 1; block < nrOfBlocks; ++block)
    {
      unsigned long long newPos = blockOffset[block + 1];

      ReadDataBlock_v6(myfile, blockReader, newPos - offset, blockSizeChar, 0, blockSizeChar - 1, vecPos);

      vecPos += blockSizeChar;
      offset = newPos;
    }

    unsigned long long newPos = blockOffset[nrOfBlocks + 1];
    ReadDataBlock_v6(myfile, blockReader, newPos - offset, nrOfElements, 0, endOffset, vecPos);

    return;
  }


  // Vector data is compressed

  unsigned int bufLength = (nrOfBlocks + 1) * CHAR_INDEX_SIZE; // 1 long and 2 unsigned int per block

  // add extra first element for convenience
  std::unique_ptr<char[]> blockInfoP = std::unique_ptr<char[]>(new char[bufLength + CHAR_INDEX_SIZE]);
  char* blockInfo = blockInfoP.get();

  if (startBlock > 0) // include previous block offset
  {
    myfile.seekg(blockPos + CHAR_HEADER_SIZE + (startBlock - 1) * CHAR_INDEX_SIZE); // jump to correct block index
    myfile.read(blockInfo, (nrOfBlocks + 1) * CHAR_INDEX_SIZE);
  }
  else
  {
    unsigned long long* firstBlock = reinterpret_cast<unsigned long long*>(blockInfo);
    *firstBlock = CHAR_HEADER_SIZE + (totNrOfBlocks + 1) * CHAR_INDEX_SIZE; // offset of first data block
    myfile.read(&blockInfo[CHAR_INDEX_SIZE], nrOfBlocks * CHAR_INDEX_SIZE);
  }

  // Get block meta data
  unsigned long long* offset = reinterpret_cast<unsigned long long*>(blockInfo);
  char* blockP = &blockInfo[CHAR_INDEX_SIZE];
  unsigned long long* curBlockPos = reinterpret_cast<unsigned long long*>(blockP);
  unsigned short int* algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  unsigned short int* algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  int* intBufSize = reinterpret_cast<int*>(blockP + 12);

  // move to first data block

  myfile.seekg(blockPos + *offset);

  unsigned long long endElem = blockSizeChar - 1;
  unsigned long long nrOfElements = blockSizeChar;

  Decompressor decompressor; // uncompresses all available algorithms

  if (startBlock == endBlock) // subset start and end of block
  {
    endElem = endOffset;
    if (endBlock == totNrOfBlocks)
    {
      nrOfElements = size - totNrOfBlocks * blockSizeChar; // last block can have less elements
    }
  }

  // Read first block with offset
  unsigned long long blockSize = *curBlockPos - *offset; // size of data block

  ReadDataBlockCompressed_v6(myfile, blockReader, blockSize, nrOfElements, startOffset, endElem, 0, *intBufSize,
    decompressor, *algoInt, *algoChar);


  if (startBlock == endBlock) // subset start and end of block
  {
    return;
  }

  // more than 1 block

  offset = curBlockPos;

  unsigned long long vecPos = blockSizeChar - startOffset;

  if (endBlock == totNrOfBlocks)
  {
    nrOfElements = size - totNrOfBlocks * blockSizeChar; // last block can have less elements
  }

  --nrOfBlocks; // iterate all but last block
  blockP += CHAR_INDEX_SIZE; // move to next index element
  for (unsigned int block = 1; block < nrOfBlocks; ++block)
  {
    curBlockPos = reinterpret_cast<unsigned long long*>(blockP);
    algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
    algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
    intBufSize = reinterpret_cast<int*>(blockP + 12);

    ReadDataBlockCompressed_v6(myfile, blockReader, *curBlockPos - *offset, blockSizeChar, 0, blockSizeChar - 1, vecPos, *intBufSize,
      decompressor, *algoInt, *algoChar);

    vecPos += blockSizeChar;
    offset = curBlockPos;
    blockP += CHAR_INDEX_SIZE; // move to next index element
  }

  curBlockPos = reinterpret_cast<unsigned long long*>(blockP);
  algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  intBufSize = reinterpret_cast<int*>(blockP + 12);

  ReadDataBlockCompressed_v6(myfile, blockReader, *curBlockPos - *offset, nrOfElements, 0, endOffset, vecPos, *intBufSize,
    decompressor, *algoInt, *algoChar);
}
