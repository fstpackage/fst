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

#include "character/character_v6.h"
#include "interface/istringwriter.h"
#include "interface/fstdefines.h"
#include <compression/compressor.h>

#include <fstream>


// #include <boost/unordered_map.hpp>

using namespace std;


inline unsigned int StoreCharBlock_v6(ofstream &myfile, IStringWriter* blockRunner, unsigned int startCount, unsigned int endCount)
{
  blockRunner->SetBuffersFromVec(startCount, endCount);

  unsigned int nrOfElements = endCount - startCount;  // the string at position endCount is not included
  unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag

  myfile.write((char*)(blockRunner->strSizes), nrOfElements * 4);  // write string lengths
  myfile.write((char*)(blockRunner->naInts), nrOfNAInts * 4);  // write string lengths

  unsigned int totSize = blockRunner->bufSize;

  myfile.write(blockRunner->activeBuf, totSize);

  return totSize + (nrOfElements + nrOfNAInts) * 4;

}


inline unsigned int storeCharBlockCompressed_v6(ofstream &myfile, IStringWriter* blockRunner, unsigned int startCount,
  unsigned int endCount, StreamCompressor* intCompressor, StreamCompressor* charCompressor, unsigned short int &algoInt,
  unsigned short int &algoChar, int &intBufSize)
{
  // Determine string lengths
  unsigned int nrOfElements = endCount - startCount;  // the string at position endCount is not included
  unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag


  // Compress string size vector
  unsigned int strSizesBufLength = nrOfElements * 4;

  // 1) Use stack buffer here !!!!!!
  // 2) compress to 1 or 2 bytes if possible with strSizesBufLength
  int bufSize = intCompressor->CompressBufferSize(strSizesBufLength);  // 1 integer per string
  char *intBuf = new char[bufSize];

  CompAlgo compAlgorithm;
  intBufSize = intCompressor->Compress(myfile, (char*)(blockRunner->strSizes), strSizesBufLength, intBuf, compAlgorithm);
  algoInt = (unsigned short int) (compAlgorithm);  // store selected algorithm

  // Write NA bits uncompressed (add compression later ?)
  myfile.write((char*)(blockRunner->naInts), nrOfNAInts * 4);  // write string lengths

  unsigned int totSize = blockRunner->bufSize;

  int compBufSize = charCompressor->CompressBufferSize(totSize);
  // unsigned int pos = 0;
  // unsigned int lastPos = 0;
  // int sizeCount = -1;
  char* compBuf = new char[compBufSize];  // character buffer   check if reuse possible?


  // Compress buffer
  int resSize = charCompressor->Compress(myfile, blockRunner->activeBuf, totSize, compBuf, compAlgorithm);

  algoChar = (unsigned short int) (compAlgorithm);  // store selected algorithm

  delete[] compBuf;
  delete[] intBuf;

  return nrOfNAInts * 4 + resSize + intBufSize;
}


void fdsWriteCharVec_v6(ofstream &myfile, IStringWriter* blockRunner, int compression)
{
  unsigned int vecLength = blockRunner->vecLength;

  unsigned long long curPos = myfile.tellp();
  unsigned int nrOfBlocks = (vecLength - 1) / BLOCKSIZE_CHAR;  // number of blocks minus 1

  if (compression == 0)
  {
    unsigned int metaSize = CHAR_HEADER_SIZE + (nrOfBlocks + 1) * 8;
    char *meta = new char[metaSize];  // first CHAR_HEADER_SIZE bytes store compression setting and block size

    // Set column header
    unsigned int* isCompressed  = (unsigned int*) meta;
    unsigned int* blockSizeChar = (unsigned int*) &meta[4];
    *blockSizeChar = BLOCKSIZE_CHAR;
    *isCompressed = 0;

    myfile.write(meta, metaSize);  // write block offset index

    unsigned long long* blockPos = (unsigned long long*) &meta[CHAR_HEADER_SIZE];
    unsigned long long fullSize = metaSize;

    for (unsigned int block = 0; block < nrOfBlocks; ++block)
    {
      unsigned int totSize = StoreCharBlock_v6(myfile, blockRunner, block * BLOCKSIZE_CHAR, (block + 1) * BLOCKSIZE_CHAR);
      fullSize += totSize;
      blockPos[block] = fullSize;
    }

    unsigned int totSize = StoreCharBlock_v6(myfile, blockRunner, nrOfBlocks * BLOCKSIZE_CHAR, vecLength);
    fullSize += totSize;
    blockPos[nrOfBlocks] = fullSize;

    myfile.seekp(curPos + CHAR_HEADER_SIZE);
    myfile.write((char*)(blockPos), (nrOfBlocks + 1) * 8);  // additional zero for index convenience
    myfile.seekp(curPos + fullSize);  // back to end of file

    delete[] meta;

    return;
  }


  // Use compression

  unsigned int metaSize = CHAR_HEADER_SIZE + (nrOfBlocks + 1) * CHAR_INDEX_SIZE;  // 1 long and 2 unsigned int per block
  char *meta = new char[metaSize];

  // Set column header
  unsigned int* isCompressed  = (unsigned int*) meta;
  unsigned int* blockSizeChar = (unsigned int*) &meta[4];
  *blockSizeChar = BLOCKSIZE_CHAR;
  *isCompressed = 1;  // set compression flag

  myfile.write(meta, metaSize);  // write block offset and algorithm index

  char* blockP = &meta[CHAR_HEADER_SIZE];

  unsigned long long fullSize = metaSize;

  // Compressors
  Compressor* compressInt;
  Compressor* compressInt2 = nullptr;
  StreamCompressor* streamCompressInt = nullptr;
  Compressor* compressChar = nullptr;
  Compressor* compressChar2 = nullptr;
  StreamCompressor* streamCompressChar;

  // Compression settings
  if (compression <= 50)
  {
    // Integer vector compressor
    compressInt = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
    streamCompressInt = new StreamLinearCompressor(compressInt, 2 * compression);

    // Character vector compressor
    compressChar = new SingleCompressor(CompAlgo::LZ4, 20);
    streamCompressChar = new StreamLinearCompressor(compressChar, 2 * compression);  // unknown blockSize
  } else  // 51 - 100
  {
    // Integer vector compressor
    compressInt = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
    compressInt2 = new SingleCompressor(CompAlgo::ZSTD_SHUF4, 0);
    streamCompressInt = new StreamCompositeCompressor(compressInt, compressInt2, 2 * (compression - 50));

    // Character vector compressor
    compressChar = new SingleCompressor(CompAlgo::LZ4, 20);
    compressChar2 = new SingleCompressor(CompAlgo::ZSTD, 20);
    streamCompressChar = new StreamCompositeCompressor(compressChar, compressChar2, 2 * (compression - 50));
  }

  for (unsigned int block = 0; block < nrOfBlocks; ++block)
  {
    unsigned long long* blockPos = (unsigned long long*) blockP;
    unsigned short int* algoInt  = (unsigned short int*) (blockP + 8);
    unsigned short int* algoChar = (unsigned short int*) (blockP + 10);
    int* intBufSize = (int*) (blockP + 12);

    blockRunner->SetBuffersFromVec(block * BLOCKSIZE_CHAR, (block + 1) * BLOCKSIZE_CHAR);
    unsigned int totSize = storeCharBlockCompressed_v6(myfile, blockRunner, block * BLOCKSIZE_CHAR,
      (block + 1) * BLOCKSIZE_CHAR, streamCompressInt, streamCompressChar, *algoInt, *algoChar, *intBufSize);

    fullSize += totSize;
    *blockPos = fullSize;
    blockP += CHAR_INDEX_SIZE;  // advance one block index entry
  }

  unsigned long long* blockPos = (unsigned long long*) blockP;
  unsigned short int* algoInt  = (unsigned short int*) (blockP + 8);
  unsigned short int* algoChar = (unsigned short int*) (blockP + 10);
  int* intBufSize = (int*) (blockP + 12);

  blockRunner->SetBuffersFromVec(nrOfBlocks * BLOCKSIZE_CHAR, vecLength);
  unsigned int totSize = storeCharBlockCompressed_v6(myfile, blockRunner, nrOfBlocks * BLOCKSIZE_CHAR,
    vecLength, streamCompressInt, streamCompressChar, *algoInt, *algoChar, *intBufSize);

  fullSize += totSize;
  *blockPos = fullSize;

  delete streamCompressInt;
  delete streamCompressChar;
  delete compressInt;
  delete compressInt2;
  delete compressChar;
  delete compressChar2;

  myfile.seekp(curPos + CHAR_HEADER_SIZE);
  myfile.write((char*)(&meta[CHAR_HEADER_SIZE]), (nrOfBlocks + 1) * CHAR_INDEX_SIZE);  // additional zero for index convenience
  myfile.seekp(0, ios_base::end);

  delete[] meta;

  return;
}


inline void ReadDataBlock_v6(istream &myfile, IStringColumn* blockReader, unsigned long long blockSize, unsigned int nrOfElements,
  unsigned int startElem, unsigned int endElem, unsigned int vecOffset)
{
  unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // last bit is NA flag
  unsigned int totElements = nrOfElements + nrOfNAInts;
  unsigned int *sizeMeta = new unsigned int[totElements];
  myfile.read((char*) sizeMeta, totElements * 4);  // read cumulative string lengths and NA bits

  unsigned int charDataSize = blockSize - totElements * 4;

  char* buf = new char[charDataSize];
  myfile.read(buf, charDataSize);  // read string lengths

  // Create IBlockReader
  // IBlockReader* blockReader = new BlockReaderChar(strVec);
  blockReader->BufferToVec(nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);
  // delete blockReader;

  // ReadDataBlockInfo_v6(strVec, nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);

  delete[] sizeMeta;
  delete[] buf;
}


inline void ReadDataBlockCompressed_v6(istream &myfile, IStringColumn* blockReader, unsigned long long blockSize, unsigned int nrOfElements,
  unsigned int startElem, unsigned int endElem, unsigned int vecOffset,
  unsigned int intBlockSize, Decompressor &decompressor, unsigned short int &algoInt, unsigned short int &algoChar)
{
  unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // NA metadata including overall NA bit
  unsigned int totElements = nrOfElements + nrOfNAInts;
  unsigned int *sizeMeta = new unsigned int[totElements];

  // Read and uncompress str sizes data
  if (algoInt == 0)  // uncompressed
  {
    myfile.read((char*) sizeMeta, totElements * 4);  // read cumulative string lengths
  }
  else
  {
    unsigned int intBufSize = intBlockSize;
    char *strSizeBuf = new char[intBufSize];
    myfile.read(strSizeBuf, intBufSize);
    myfile.read((char*) &sizeMeta[nrOfElements], nrOfNAInts * 4);  // read cumulative string lengths

    // Decompress size but not NA metadata (which is currently uncompressed)

    decompressor.Decompress(algoInt, (char*) sizeMeta, nrOfElements * 4, strSizeBuf, intBlockSize);

    delete[] strSizeBuf;
  }

  unsigned int charDataSizeUncompressed = sizeMeta[nrOfElements - 1];

  // Read and uncompress string vector data, use stack if possible here !!!!!
  unsigned int charDataSize = blockSize - intBlockSize - nrOfNAInts * 4;
  char* buf = new char[charDataSizeUncompressed];

  if (algoChar == 0)
  {
    myfile.read(buf, charDataSize);  // read string lengths
  }
  else
  {
    char* bufCompressed = new char[charDataSize];
    myfile.read(bufCompressed, charDataSize);  // read string lengths
    decompressor.Decompress(algoChar, buf, charDataSizeUncompressed, bufCompressed, charDataSize);
    delete[] bufCompressed;
  }

  blockReader->BufferToVec(nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);

  delete[] buf;  // character vector buffer
  delete[] sizeMeta;
}


void fdsReadCharVec_v6(istream &myfile, IStringColumn* blockReader, unsigned long long blockPos, unsigned int startRow, unsigned int vecLength, unsigned int size)
{
  // Jump to startRow size
  myfile.seekg(blockPos);

  // Read algorithm type and block size
  unsigned int meta[2];
  myfile.read((char*) meta, CHAR_HEADER_SIZE);

  unsigned int blockSizeChar = meta[1];
  unsigned int totNrOfBlocks = (size - 1) / blockSizeChar;  // total number of blocks minus 1
  unsigned int startBlock = startRow / blockSizeChar;
  unsigned int startOffset = startRow - (startBlock * blockSizeChar);
  unsigned int endBlock = (startRow + vecLength - 1)  / blockSizeChar;
  unsigned int endOffset = (startRow + vecLength - 1)  -  endBlock *blockSizeChar;
  unsigned int nrOfBlocks = 1 + endBlock - startBlock;  // total number of blocks to read

  // Create result vector
  blockReader->AllocateVec(vecLength);

  // Vector data is uncompressed

  if (meta[0] == 0)
  {
    unsigned long long *blockOffset = new unsigned long long[1 + nrOfBlocks];  // block positions

    if (startBlock > 0)  // include previous block offset
    {
      myfile.seekg(blockPos + CHAR_HEADER_SIZE + (startBlock - 1) * 8);  // jump to correct block index
      myfile.read((char*) blockOffset, (1 + nrOfBlocks) * 8);
    }
    else
    {
      blockOffset[0] = CHAR_HEADER_SIZE + (totNrOfBlocks + 1) * 8;
      myfile.read((char*) &blockOffset[1], nrOfBlocks * 8);
    }


    // Navigate to first selected data block
    unsigned long long offset = blockOffset[0];
    myfile.seekg(blockPos + offset);

    unsigned int endElem = blockSizeChar - 1;
    unsigned int nrOfElements = blockSizeChar;

    if (startBlock == endBlock)  // subset start and end of block
    {
      endElem = endOffset;
      if (endBlock == totNrOfBlocks)
      {
        nrOfElements = size - totNrOfBlocks * blockSizeChar;  // last block can have less elements
      }
    }

    // Read first block with offset
    unsigned long long blockSize = blockOffset[1] - offset;  // size of data block

    ReadDataBlock_v6(myfile, blockReader, blockSize, nrOfElements, startOffset, endElem, 0);

    if (startBlock == endBlock)  // subset start and end of block
    {
      delete[] blockOffset;

      return;
    }

    offset = blockOffset[1];
    unsigned int vecPos = blockSizeChar - startOffset;

    if (endBlock == totNrOfBlocks)
    {
      nrOfElements = size - totNrOfBlocks * blockSizeChar;  // last block can have less elements
    }

    --nrOfBlocks;  // iterate full blocks
    for (unsigned int block = 1; block < nrOfBlocks; ++block)
    {
      unsigned long long newPos = blockOffset[block + 1];

      ReadDataBlock_v6(myfile, blockReader, newPos - offset, blockSizeChar, 0, blockSizeChar - 1, vecPos);

      vecPos += blockSizeChar;
      offset = newPos;
    }

    unsigned long long newPos = blockOffset[nrOfBlocks + 1];
    ReadDataBlock_v6(myfile, blockReader, newPos - offset, nrOfElements, 0, endOffset, vecPos);

    delete[] blockOffset;

    return;
  }


  // Vector data is compressed

  unsigned int bufLength = (nrOfBlocks + 1) * CHAR_INDEX_SIZE;  // 1 long and 2 unsigned int per block
  char *blockInfo = new char[bufLength + CHAR_INDEX_SIZE];  // add extra first element for convenience


  if (startBlock > 0)  // include previous block offset
  {
    myfile.seekg(blockPos + CHAR_HEADER_SIZE + (startBlock - 1) * CHAR_INDEX_SIZE);  // jump to correct block index
    myfile.read(blockInfo, (nrOfBlocks + 1) * CHAR_INDEX_SIZE);
  }
  else
  {
    unsigned long long* firstBlock = (unsigned long long*) blockInfo;
    *firstBlock = CHAR_HEADER_SIZE + (totNrOfBlocks + 1) * CHAR_INDEX_SIZE;  // offset of first data block
    myfile.read(&blockInfo[CHAR_INDEX_SIZE], nrOfBlocks * CHAR_INDEX_SIZE);
  }

  // Get block meta data
  unsigned long long* offset = (unsigned long long*) blockInfo;
  char* blockP = &blockInfo[CHAR_INDEX_SIZE];
  unsigned long long* curBlockPos = (unsigned long long*) blockP;
  unsigned short int* algoInt  = (unsigned short int*) (blockP + 8);
  unsigned short int* algoChar = (unsigned short int*) (blockP + 10);
  int* intBufSize = (int*) (blockP + 12);

  // move to first data block

  myfile.seekg(blockPos + *offset);

  unsigned int endElem = blockSizeChar - 1;
  unsigned int nrOfElements = blockSizeChar;

  if (startBlock == endBlock)  // subset start and end of block
  {
    endElem = endOffset;
    if (endBlock == totNrOfBlocks)
    {
      nrOfElements = size - totNrOfBlocks * blockSizeChar;  // last block can have less elements
    }
  }

  // Read first block with offset
  unsigned long long blockSize = *curBlockPos - *offset;  // size of data block

  Decompressor decompressor;  // uncompress all availble algorithms

  ReadDataBlockCompressed_v6(myfile, blockReader, blockSize, nrOfElements, startOffset, endElem, 0, *intBufSize,
                             decompressor, *algoInt, *algoChar);


  if (startBlock == endBlock)  // subset start and end of block
  {
    delete[] blockInfo;

    return;
  }

  offset = curBlockPos;

  unsigned int vecPos = blockSizeChar - startOffset;

  if (endBlock == totNrOfBlocks)
  {
    nrOfElements = size - totNrOfBlocks * blockSizeChar;  // last block can have less elements
  }

  --nrOfBlocks;  // iterate all but last block
  blockP += CHAR_INDEX_SIZE;  // move to next index element
  for (unsigned int block = 1; block < nrOfBlocks; ++block)
  {
    unsigned long long* curBlockPos = (unsigned long long*) blockP;
    unsigned short int* algoInt  = (unsigned short int*) (blockP + 8);
    unsigned short int* algoChar = (unsigned short int*) (blockP + 10);
    int* intBufSize = (int*) (blockP + 12);

    ReadDataBlockCompressed_v6(myfile, blockReader, *curBlockPos - *offset, blockSizeChar, 0, blockSizeChar - 1, vecPos, *intBufSize,
      decompressor, *algoInt, *algoChar);

    vecPos += blockSizeChar;
    offset = curBlockPos;
    blockP += CHAR_INDEX_SIZE;  // move to next index element
  }

  curBlockPos = (unsigned long long*) blockP;
  algoInt  = (unsigned short int*) (blockP + 8);
  algoChar = (unsigned short int*) (blockP + 10);
  intBufSize = (int*) (blockP + 12);

  ReadDataBlockCompressed_v6(myfile, blockReader, *curBlockPos - *offset, nrOfElements, 0, endOffset, vecPos, *intBufSize,
    decompressor, *algoInt, *algoChar);

  delete[] blockInfo;

  return;
}
