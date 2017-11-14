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


#ifndef FST_COMPRESSOR_H
#define FST_COMPRESSOR_H


#include <stdlib.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>

#include "compression/compressor.h"
#include "interface/fstdefines.h"
#include "interface/itypefactory.h"
#include "interface/openmphelper.h"

#include "ZSTD/common/xxhash.h"
#include <memory>

enum COMPRESSION_ALGORITHM
{
  ALGORITHM_NONE = 0,
  ALGORITHM_LZ4,
  ALGORITHM_ZSTD
};


class FstCompressor
{
  std::unique_ptr<Compressor> compressor;
  ITypeFactory* typeFactory;
  COMPRESSION_ALGORITHM compAlgorithm;

public:

  FstCompressor(ITypeFactory* typeFactory)
  {
    this->typeFactory = typeFactory;
  }

  ~FstCompressor()
  {
  }

  /**
	 * \brief constructor for a FstCompressor object
	 * \param compAlgorithm algorithm to use for compression
	 * \param compressionLevel compression level between 0 and 100
	 * \param typeFactory type factory to create a IBlobContainer
	 */
  FstCompressor(COMPRESSION_ALGORITHM compAlgorithm, unsigned int compressionLevel, ITypeFactory* typeFactory)
  {
    this->typeFactory = typeFactory;
    this->compAlgorithm = compAlgorithm;

    switch (compAlgorithm)
    {
    case COMPRESSION_ALGORITHM::ALGORITHM_LZ4:
      {
        compressor = std::unique_ptr<Compressor>(new SingleCompressor(CompAlgo::LZ4, compressionLevel));
        break;
      }

    case COMPRESSION_ALGORITHM::ALGORITHM_ZSTD:
      {
        compressor = std::unique_ptr<Compressor>(new SingleCompressor(CompAlgo::ZSTD, compressionLevel));
        break;
      }

    default:
      {
        compressor = std::unique_ptr<Compressor>(new SingleCompressor(CompAlgo::LZ4, compressionLevel));
        break;
      }
    }
  }


  /**
   * \brief Compress data with the 'LZ4' or 'ZSTD' compressor.
   * \param blobSource source buffer to compress.
   * \param blobLength Length of source buffer.
   */
  IBlobContainer* CompressBlob(unsigned char* blobSource, unsigned long long blobLength, bool hash = true) const
  {
    int nrOfThreads = GetFstThreads();

    // Check for empty source
    if (blobLength == 0)
    {
      throw(std::runtime_error(FSTERROR_COMP_NO_DATA));
    }


    // block size to use for compression has a lower bound for compression efficiency
    unsigned long long minBlock = std::max(static_cast<unsigned long long>(BLOCKSIZE),
                                           1 + (blobLength - 1) / PREV_NR_OF_BLOCKS);

    // And a higher bound for compressor compatability
    unsigned int blockSize = static_cast<unsigned int>(std::min(minBlock, static_cast<unsigned long long>(INT_MAX)));

    int nrOfBlocks = static_cast<int>(1 + (blobLength - 1) / blockSize);
    nrOfThreads = std::min(nrOfThreads, nrOfBlocks);

    unsigned int maxCompressSize = this->compressor->CompressBufferSize(blockSize);
    unsigned int lastBlockSize = 1 + (blobLength - 1) % blockSize;

    unsigned long long bufSize = nrOfBlocks * maxCompressSize;
    float blocksPerThread = static_cast<float>(nrOfBlocks) / nrOfThreads;

    // Compressed sizes
    std::unique_ptr<unsigned long long[]> compSizesP(new unsigned long long[nrOfBlocks + 1]);
    unsigned long long* compSizes = compSizesP.get();

    std::unique_ptr<unsigned long long[]> compBatchSizesP(new unsigned long long[nrOfThreads]);
    unsigned long long* compBatchSizes = compBatchSizesP.get();

    std::unique_ptr<unsigned long long[]> blockHashesP;

    if (hash)
    {
      blockHashesP = std::unique_ptr<unsigned long long[]>(new unsigned long long[nrOfBlocks]);
    }

    unsigned long long* blockHashes = blockHashesP.get();

    // We need nrOfBlocks buffers
    unsigned int compressionAlgo;

    std::unique_ptr<unsigned char[]> calcBufferP(new unsigned char[bufSize]);
    unsigned char* calcBuffer = calcBufferP.get();

#pragma omp parallel num_threads(nrOfThreads) shared(compSizes,nrOfThreads,blocksPerThread,maxCompressSize,lastBlockSize,compBatchSizes)
    {
#pragma omp for schedule(static, 1) nowait
      for (int blockBatch = 0; blockBatch < (nrOfThreads - 1); blockBatch++) // all but last batch
      {
        CompAlgo compAlgo;
        float blockOffset = blockBatch * blocksPerThread;
        int blockNr = static_cast<int>(0.00001 + blockOffset);
        int nextblockNr = static_cast<int>(blocksPerThread + 0.00001 + blockOffset);

        unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr; // buffer for compression results of current thread

        // inner loop is executed by current thread
        unsigned long long bufPos = 0;
        for (int block = blockNr; block < nextblockNr; block++)
        {
          int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
                                                    reinterpret_cast<char*>(&blobSource[block * blockSize]), blockSize, compAlgo);
          compSizes[block] = static_cast<unsigned long long>(compSize);

          // Hash compression result
          if (hash)
          {
            blockHashes[block] = XXH64(threadBuf + bufPos, compSize, FST_HASH_SEED);
          }

          bufPos += compSize;
        }

        compBatchSizes[blockBatch] = bufPos;
      }

#pragma omp single
      {
        CompAlgo compAlgo;
        int blockNr = static_cast<int>(0.00001 + (nrOfThreads - 1) * blocksPerThread);
        int nextblockNr = static_cast<int>(0.00001 + (nrOfThreads * blocksPerThread)) - 1; // exclude last block

        unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr; // buffer for compression results of current thread

        // inner loop is executed by current thread
        unsigned long long bufPos = 0;
        for (int block = blockNr; block < nextblockNr; block++)
        {
          int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
                                                    reinterpret_cast<char*>(&blobSource[block * blockSize]), blockSize, compAlgo);
          compSizes[block] = static_cast<unsigned long long>(compSize);

          // Hash compression result
          if (hash)
          {
            blockHashes[block] = XXH64(threadBuf + bufPos, compSize, FST_HASH_SEED);
          }

          bufPos += compSize;
        }

        // last block
        int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
                                                  reinterpret_cast<char*>(&blobSource[nextblockNr * blockSize]), lastBlockSize, compAlgo);
        compSizes[nextblockNr] = static_cast<unsigned long long>(compSize);

        compBatchSizes[nrOfThreads - 1] = bufPos + compSize;
        compressionAlgo = static_cast<unsigned int>(compAlgo);

        // Hash compression result
        if (hash)
        {
          blockHashes[nextblockNr] = XXH64(threadBuf + bufPos, compSize, FST_HASH_SEED);
        }
      }
    } // end parallel region and join all threads


    unsigned long long allBlockHash = 0;

    if (hash)
    {
      allBlockHash = XXH64(blockHashes, nrOfBlocks * 8, FST_HASH_SEED);
    }

    unsigned long long totCompSize = 0;
    for (int blockBatch = 0; blockBatch < nrOfThreads; blockBatch++)
    {
      totCompSize += compBatchSizes[blockBatch];
    }

    // In memory compression format:
    // Size                 | Type               | Description
    // 4                    | insigned int       | header hash
    // 4                    | unsigned int       | blockSize
    // 4                    | unsigned int       | version
    // 4                    | unsigned int       | COMPRESSION_ALGORITHM and upper bit isHashed
    // 8                    | unsigned long long | vecLength
    // 8                    | unsigned long long | block hash result
    // 8 * (nrOfBlocks + 1) | unsigned long long | block offset
    //                      | unsigned char      | compressed data

    unsigned long long headerSize = 8 * (nrOfBlocks + 5);
    IBlobContainer* blobContainer = typeFactory->CreateBlobContainer(headerSize + totCompSize);
    unsigned char* blobData = blobContainer->Data();

    unsigned int* headerHash = reinterpret_cast<unsigned int*>(blobData);
    unsigned int* pBlockSize = reinterpret_cast<unsigned int*>(blobData + 4);
    unsigned int* version = reinterpret_cast<unsigned int*>(blobData + 8);
    unsigned int* algo = reinterpret_cast<unsigned int*>(blobData + 12);
    unsigned long long* vecLength = reinterpret_cast<unsigned long long*>(blobData + 16);
    unsigned long long* hashResult = reinterpret_cast<unsigned long long*>(blobData + 24);
    unsigned long long* blockOffsets = reinterpret_cast<unsigned long long*>(blobData + 32);

    *pBlockSize = blockSize;
    *version = 1;
    *algo = compressionAlgo | ((1 << 31) * hash); // upper bit signals isHashed
    *vecLength = blobLength;
    *hashResult = allBlockHash;

    // calculate offsets for memcpy
    unsigned long long dataOffset = headerSize;

    std::unique_ptr<unsigned long long[]> dataOffsetsP(new unsigned long long[nrOfThreads]);
    unsigned long long* dataOffsets = dataOffsetsP.get();

    for (int blockBatch = 0; blockBatch < nrOfThreads; blockBatch++)
    {
      dataOffsets[blockBatch] = dataOffset;
      dataOffset += compBatchSizes[blockBatch];
    }

    // multi-threaded memcpy
#pragma omp parallel for schedule(static, 1)
    for (int blockBatch = 0; blockBatch < nrOfThreads; blockBatch++)
    {
      float blockOffset = blockBatch * blocksPerThread;
      int blockNr = static_cast<int>(0.00001 + blockOffset);
      unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr; // buffer for compression results of current thread
      std::memcpy(blobData + dataOffsets[blockBatch], threadBuf, compBatchSizes[blockBatch]);
    } // end parallel region

    unsigned long long blockOffset = headerSize;
    for (int block = 0; block < nrOfBlocks; block++)
    {
      blockOffsets[block] = blockOffset;
      blockOffset += compSizes[block];
    }
    blockOffsets[nrOfBlocks] = blockOffset;

    *headerHash = XXH32(&blobData[4], headerSize - 4, FST_HASH_SEED); // header hash

    return blobContainer;
  }


  IBlobContainer* DecompressBlob(unsigned char* blobSource, unsigned long long blobLength, bool checkHashes = true) const
  {
    Decompressor decompressor;
    int nrOfThreads = GetFstThreads(); // available threads

    // Minimum length of compressed data format
    if (blobLength < 45)
    {
      throw(std::runtime_error(FSTERROR_COMP_SIZE));
    }

    // In memory compression format:
    // Size                 | Type               | Description
    // 4                    | insigned int       | header hash
    // 4                    | unsigned int       | blockSize
    // 4                    | unsigned int       | version
    // 4                    | unsigned int       | COMPRESSION_ALGORITHM and upper bit isHashed
    // 8                    | unsigned long long | vecLength
    // 8                    | unsigned long long | block hash result
    // 8 * (nrOfBlocks + 1) | unsigned long long | block offset
    //                      | unsigned char      | compressed data

    // Meta data of compressed blocks

    unsigned int* headerHash = reinterpret_cast<unsigned int*>(blobSource);
    unsigned int* blockSize = reinterpret_cast<unsigned int*>(blobSource + 4);
    //unsigned int* version            = reinterpret_cast<unsigned int*>(blobSource + 8);
    unsigned int* algo = reinterpret_cast<unsigned int*>(blobSource + 12);
    unsigned long long* vecLength = reinterpret_cast<unsigned long long*>(blobSource + 16);
    unsigned long long* hashResult = reinterpret_cast<unsigned long long*>(blobSource + 24);
    unsigned long long* blockOffsets = reinterpret_cast<unsigned long long*>(blobSource + 32);

    bool hash = checkHashes && static_cast<bool>(((*algo) >> 31) & 1);
    unsigned int algorithm = (*algo) & 0x7fffffff; // highest bit signals hash

    // Calculate number of blocks
    int nrOfBlocks = static_cast<int>(1 + (*vecLength - 1) / *blockSize); // including (partial) last

    unsigned long long headerSize = 8 * (static_cast<unsigned long long>(nrOfBlocks) + 5);

    // Minimum length of compressed data format including block offset header information
    if (blobLength <= headerSize)
    {
      throw(std::runtime_error(FSTERROR_COMP_SIZE)); // TODO: error in fstdefines.h
    }

    unsigned int headHash = XXH32(&blobSource[4], headerSize - 4, FST_HASH_SEED); // header hash

    if (*headerHash != headHash)
    {
      throw(std::runtime_error(FSTERROR_COMP_HEADER));
    }


    // Version checks here

    // Create result blob
    IBlobContainer* blobContainer = typeFactory->CreateBlobContainer(*vecLength); // create result blob
    unsigned char* blobData = blobContainer->Data();

    // Source vector has correct length
    if (blockOffsets[nrOfBlocks] != blobLength)
    {
      delete blobContainer;
      throw(std::runtime_error(FSTERROR_COMP_SIZE));
    }

    // Determine required number of threads
    nrOfThreads = std::min(nrOfBlocks, nrOfThreads);

    // Determine number of blocks per (thread) batch
    float batchFactor = static_cast<float>(nrOfBlocks) / nrOfThreads;

    bool error = false;

    if (hash)
    {
      std::unique_ptr<unsigned long long[]> blockHashesP(new unsigned long long[nrOfBlocks]);
      unsigned long long* blockHashes = blockHashesP.get();

#pragma omp parallel num_threads(nrOfThreads)
      {
#pragma omp for schedule(static, 1) nowait
        for (int batch = 0; batch < (nrOfThreads - 1); batch++) // all but last batch
        {
          int fromBlock = static_cast<int>(batch * batchFactor + 0.000001); // start block
          int toBlock = static_cast<int>((batch + 1) * batchFactor + 0.000001); // end block

          // iterate block range
          for (int block = fromBlock; block < toBlock; block++)
          {
            unsigned long long blockStart = blockOffsets[block];
            unsigned long long blockEnd = blockOffsets[block + 1];

            blockHashes[block] = XXH64(blobSource + blockStart, blockEnd - blockStart, FST_HASH_SEED);
          }
        }

#pragma omp single
        {
          int fromBlock = static_cast<int>((nrOfThreads - 1) * batchFactor + 0.000001); // start block
          int toBlock = static_cast<int>(nrOfThreads * batchFactor + 0.000001); // end block

          // iterate block range with full blocks
          for (int block = fromBlock; block < (toBlock - 1); block++)
          {
            unsigned long long blockStart = blockOffsets[block];
            unsigned long long blockEnd = blockOffsets[block + 1];

            blockHashes[block] = XXH64(blobSource + blockStart, blockEnd - blockStart, FST_HASH_SEED);
          }

          // last block
          unsigned long long blockStart = blockOffsets[toBlock - 1];
          unsigned long long blockEnd = blockOffsets[toBlock];

          blockHashes[toBlock - 1] = XXH64(blobSource + blockStart, blockEnd - blockStart, FST_HASH_SEED);
        }
      }

      unsigned long long totHashes = XXH64(blockHashes, 8 * nrOfBlocks, FST_HASH_SEED);

      if (totHashes != *hashResult)
      {
        delete blobContainer;
        throw(std::runtime_error(FSTERROR_COMP_DATA_HASH));
      }
    }

#pragma omp parallel num_threads(nrOfThreads)
    {
#pragma omp for schedule(static, 1) nowait
      for (int batch = 0; batch < (nrOfThreads - 1); batch++) // all but last batch
      {
        int fromBlock = static_cast<int>(batch * batchFactor + 0.000001); // start block
        int toBlock = static_cast<int>((batch + 1) * batchFactor + 0.000001); // end block

        // iterate block range
        for (int block = fromBlock; block < toBlock; block++)
        {
          unsigned long long blockStart = blockOffsets[block];
          unsigned long long blockEnd = blockOffsets[block + 1];

          unsigned int errorCode = decompressor.Decompress(algorithm, reinterpret_cast<char*>(blobData) + *blockSize * block,
                                                           *blockSize, reinterpret_cast<const char*>(blobSource + blockStart), blockEnd - blockStart);

          if (errorCode != 0)
          {
            error = true;
          }
        }
      }

#pragma omp single
      {
        int fromBlock = static_cast<int>((nrOfThreads - 1) * batchFactor + 0.000001); // start block
        int toBlock = static_cast<int>(nrOfThreads * batchFactor + 0.000001); // end block

        // iterate block range with full blocks
        for (int block = fromBlock; block < (toBlock - 1); block++)
        {
          unsigned long long blockStart = blockOffsets[block];
          unsigned long long blockEnd = blockOffsets[block + 1];
          unsigned int errorCode = decompressor.Decompress(algorithm, reinterpret_cast<char*>(blobData) + *blockSize * block, *blockSize,
                                                           reinterpret_cast<const char*>(blobSource + blockStart), blockEnd - blockStart);

          if (errorCode != 0)
          {
            error = true;
          }
        }

        // last block
        int lastBlockSize = 1 + (*vecLength - 1) % *blockSize;
        unsigned long long blockStart = blockOffsets[toBlock - 1];
        unsigned long long blockEnd = blockOffsets[toBlock];
        unsigned int errorCode = decompressor.Decompress(algorithm, reinterpret_cast<char*>(blobData) + *blockSize * (toBlock - 1), lastBlockSize,
                                                         reinterpret_cast<const char*>(blobSource + blockStart), blockEnd - blockStart);

        if (errorCode != 0)
        {
          error = true;
        }
      }
    }


    if (error)
    {
      delete blobContainer;
      throw(std::runtime_error(FSTERROR_COMP_STREAM));
    }

    return blobContainer;
  }
};


#endif  // FST_COMPRESSOR_H
