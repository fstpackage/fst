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
#include <climits>

#include "compression/compressor.h"
#include "interface/fstdefines.h"
#include "interface/itypefactory.h"
#include "interface/openmphelper.h"

#include "xxhash.h"
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
    this->compAlgorithm = COMPRESSION_ALGORITHM::ALGORITHM_NONE;
  }

  ~FstCompressor()
  {
  }

  /**
	 * \brief constructor for a FstCompressor object
	 * \param comp_algorithm algorithm to use for compression
	 * \param compressionLevel compression level between 0 and 100
	 * \param typeFactory type factory to create a IBlobContainer
	 */
  FstCompressor(const COMPRESSION_ALGORITHM comp_algorithm, const unsigned int compressionLevel, ITypeFactory* typeFactory)
  {
    this->typeFactory = typeFactory;
    this->compAlgorithm = comp_algorithm;

    switch (comp_algorithm)
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
   * \param blob_source source buffer to compress.
   * \param blob_length Length of source buffer.
   * \param hash if true, hashes are used in the compression result
   */
  IBlobContainer* CompressBlob(unsigned char* blob_source, const unsigned long long blob_length, const bool hash = true) const
  {
    int nr_of_threads = GetFstThreads();

    // Check for empty source
    if (blob_length == 0)
    {
      throw(std::runtime_error(FSTERROR_COMP_NO_DATA));
    }

    // block size to use for compression has a 16 kB lower bound for compression efficiency
    const unsigned long long min_block = std::max(static_cast<unsigned long long>(BLOCKSIZE), 1ULL + (blob_length - 1ULL) / PREV_NR_OF_BLOCKS);

    // higher bound for compressor compatibility
	// the factor 0.8 is to avoid compressed sizes larger than INT_MAX
	// (assuming that 25% overhead will not occur in any compressor)
    const unsigned long long block_size = static_cast<unsigned long long>(std::min(min_block, static_cast<unsigned long long>(0.8 * INT_MAX)));

	// number of blocks in the compressed result
    const unsigned long long nr_of_blocks = 1 + (blob_length - 1) / block_size;

	// do not use more threads than necessary or available
	// if there are more block than threads, the default thread count is used
	// each thread runs a single block batch
    nr_of_threads = std::min(nr_of_threads, static_cast<int>(nr_of_blocks));

    const int max_compress_size = this->compressor->CompressBufferSize(block_size);
    const unsigned long long last_block_size = 1 + (blob_length - 1) % block_size;

	// this requires three times the original vector in memory
	// perhaps a max number of blocks would be better (smaller buffer)
    const unsigned long long buf_size = nr_of_blocks * static_cast<unsigned long long>(max_compress_size);
    const double blocks_per_thread = static_cast<double>(nr_of_blocks) / nr_of_threads;

    // Compressed sizes
	// can be unsigned integer as the block_size is at most INT_MAX ?
    std::unique_ptr<unsigned long long[]> comp_sizes_p(new unsigned long long[nr_of_blocks + 1]);
    unsigned long long* comp_sizes = comp_sizes_p.get();

    std::unique_ptr<unsigned long long[]> comp_batch_sizes_p(new unsigned long long[nr_of_threads]);
    unsigned long long* comp_batch_sizes = comp_batch_sizes_p.get();

	// each block has it's own hash calculated
    std::unique_ptr<unsigned long long[]> block_hashes_p;

    if (hash)
    {
      block_hashes_p = std::unique_ptr<unsigned long long[]>(new unsigned long long[nr_of_blocks]);
    }

    unsigned long long* block_hashes = block_hashes_p.get();

    // We need nr_of_blocks buffers
    unsigned int compression_algorithm;

    std::unique_ptr<unsigned char[]> calc_buffer_p(new unsigned char[buf_size]);
    unsigned char* calc_buffer = calc_buffer_p.get();

#pragma omp parallel num_threads(nr_of_threads) shared(comp_sizes,nr_of_threads,comp_batch_sizes)
    {
#pragma omp for schedule(static, 1) nowait
      for (int block_batch = 0; block_batch < (nr_of_threads - 1); block_batch++) // all but last batch
      {
        CompAlgo comp_algo;
        const double block_offset = block_batch * blocks_per_thread;
        const int block_nr = static_cast<int>(DOUBLE_DELTA + block_offset);
        const int next_block_nr = static_cast<int>(blocks_per_thread + DOUBLE_DELTA + block_offset);

		// buffer for compression results of current thread
        unsigned char* thread_buf = calc_buffer + static_cast<unsigned long long>(max_compress_size) * block_nr;

        // inner loop is executed by current thread
        unsigned long long buf_pos = 0;
        for (int block = block_nr; block < next_block_nr; block++)
        {
			// error when > 4 GB !
			const int comp_size = this->compressor->Compress(reinterpret_cast<char*>(thread_buf + buf_pos), max_compress_size,
				reinterpret_cast<char*>(&blob_source[block_size * static_cast<unsigned long long>(block)]), block_size, comp_algo);

			// this cast is a waste of format space at it is at most INT_MAX!
			comp_sizes[block] = static_cast<unsigned long long>(comp_size);

			// Hash compression result
			if (hash)
			{
				block_hashes[block] = XXH64(thread_buf + buf_pos, comp_size, FST_HASH_SEED);
			}

			buf_pos += comp_size;
		}

		comp_batch_sizes[block_batch] = buf_pos;
      }

// last thread might have a smaller task (and is executed in parallel with above loop)
#pragma omp single
      {
        CompAlgo comp_algo;

		const double block_offset = (nr_of_threads - 1) * blocks_per_thread;
      	const int block_nr = static_cast<int>(DOUBLE_DELTA + block_offset);
        const int next_block_nr = static_cast<int>(blocks_per_thread + DOUBLE_DELTA + block_offset) - 1; // exclude last block

		// buffer for compression results of current thread
		unsigned char* thread_buf = calc_buffer + static_cast<unsigned long long>(max_compress_size) * block_nr;

        // might not be executed if blocks_per_thread equals 1
        unsigned long long buf_pos = 0;
        for (int block = block_nr; block < next_block_nr; block++)
        {
          const int comp_size = this->compressor->Compress(reinterpret_cast<char*>(&thread_buf[buf_pos]), max_compress_size,
            reinterpret_cast<char*>(&blob_source[block_size * static_cast<unsigned long long>(block)]), block_size, comp_algo);
          comp_sizes[block] = static_cast<unsigned long long>(comp_size);

          // Hash compression result
          if (hash)
          {
            block_hashes[block] = XXH64(thread_buf + buf_pos, comp_size, FST_HASH_SEED);
          }

          buf_pos += comp_size;
        }

        // last block might be smaller
        const int comp_size = this->compressor->Compress(reinterpret_cast<char*>(&thread_buf[buf_pos]), max_compress_size,
          reinterpret_cast<char*>(&blob_source[block_size * static_cast<unsigned long long>(next_block_nr)]), last_block_size, comp_algo);
        comp_sizes[next_block_nr] = static_cast<unsigned long long>(comp_size);

        comp_batch_sizes[nr_of_threads - 1] = buf_pos + comp_size;
        compression_algorithm = static_cast<unsigned int>(comp_algo);

        // Hash compression result
        if (hash)
        {
          block_hashes[next_block_nr] = XXH64(thread_buf + buf_pos, comp_size, FST_HASH_SEED);
        }
      }
    } // end parallel region and join all threads


    unsigned long long allBlockHash = 0;

    // calculate hask of block hashes
    if (hash)
    {
      allBlockHash = XXH64(block_hashes, nr_of_blocks * 8, FST_HASH_SEED);
    }

    unsigned long long tot_comp_size = 0;
    for (int block_batch = 0; block_batch < nr_of_threads; block_batch++)
    {
      tot_comp_size += comp_batch_sizes[block_batch];
    }

    // In memory compression format:
    // Size                 | Type               | Description
    // 4                    | unsigned int       | header hash
    // 4                    | unsigned int       | blockSize
    // 4                    | unsigned int       | version
    // 4                    | unsigned int       | COMPRESSION_ALGORITHM and upper bit isHashed
    // 8                    | unsigned long long | vecLength
    // 8                    | unsigned long long | block hash result
    // 8 * (nrOfBlocks + 1) | unsigned long long | block offset
    //                      | unsigned char      | compressed data

    const unsigned long long headerSize = 8 * (nr_of_blocks + 5);
    IBlobContainer* blobContainer = typeFactory->CreateBlobContainer(headerSize + tot_comp_size);
    unsigned char* blobData = blobContainer->Data();

    unsigned int* headerHash = reinterpret_cast<unsigned int*>(blobData);
    unsigned int* pBlockSize = reinterpret_cast<unsigned int*>(blobData + 4);
    unsigned int* version = reinterpret_cast<unsigned int*>(blobData + 8);
    unsigned int* algo = reinterpret_cast<unsigned int*>(blobData + 12);
    unsigned long long* vecLength = reinterpret_cast<unsigned long long*>(blobData + 16);
    unsigned long long* hashResult = reinterpret_cast<unsigned long long*>(blobData + 24);
    unsigned long long* blockOffsets = reinterpret_cast<unsigned long long*>(blobData + 32);

    *pBlockSize = block_size;
    *version = FST_COMPRESS_VERSION;
    *algo = compression_algorithm | ((1 << 31) * hash); // upper bit signals isHashed
    *vecLength = blob_length;
    *hashResult = allBlockHash;

    // calculate offsets for memcpy
    unsigned long long dataOffset = headerSize;

    std::unique_ptr<unsigned long long[]> dataOffsetsP(new unsigned long long[nr_of_threads]);
    unsigned long long* dataOffsets = dataOffsetsP.get();

    for (int blockBatch = 0; blockBatch < nr_of_threads; blockBatch++)
    {
      dataOffsets[blockBatch] = dataOffset;
      dataOffset += comp_batch_sizes[blockBatch];
    }

    // multi-threaded memcpy
#pragma omp parallel for schedule(static, 1)
    for (int blockBatch = 0; blockBatch < nr_of_threads; blockBatch++)
    {
      const double blockOffset = blockBatch * blocks_per_thread;
      const int blockNr = static_cast<int>(DOUBLE_DELTA + blockOffset);
      unsigned char* threadBuf = calc_buffer + static_cast<unsigned long long>(max_compress_size) * blockNr; // buffer for compression results of current thread
      std::memcpy(blobData + dataOffsets[blockBatch], threadBuf, comp_batch_sizes[blockBatch]);
    } // end parallel region

    unsigned long long blockOffset = headerSize;
    for (unsigned int block = 0; block != nr_of_blocks; block++)
    {
      blockOffsets[block] = blockOffset;
      blockOffset += comp_sizes[block];
    }
    blockOffsets[nr_of_blocks] = blockOffset;

    *headerHash = XXH32(&blobData[4], headerSize - 4, FST_HASH_SEED); // header hash

    return blobContainer;
  }


  IBlobContainer* DecompressBlob(unsigned char* blobSource, const unsigned long long blobLength, const bool checkHashes = true) const
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

	unsigned int* version = reinterpret_cast<unsigned int*>(blobSource + 8);
	
    unsigned int* algo = reinterpret_cast<unsigned int*>(blobSource + 12);
    unsigned long long* vecLength = reinterpret_cast<unsigned long long*>(blobSource + 16);
    unsigned long long* hashResult = reinterpret_cast<unsigned long long*>(blobSource + 24);
    unsigned long long* blockOffsets = reinterpret_cast<unsigned long long*>(blobSource + 32);

    const bool hash = checkHashes && static_cast<bool>(((*algo) >> 31) & 1);
    const unsigned int algorithm = (*algo) & 0x7fffffff; // highest bit signals hash

    // Calculate number of blocks
    const int nrOfBlocks = static_cast<int>(1 + (*vecLength - 1) / *blockSize); // including (partial) last

    const unsigned long long headerSize = 8 * (static_cast<unsigned long long>(nrOfBlocks) + 5);

    // Minimum length of compressed data format including block offset header information
    if (blobLength <= headerSize)
    {
      throw(std::runtime_error(FSTERROR_COMP_SIZE));
    }

    const unsigned int headHash = XXH32(&blobSource[4], headerSize - 4, FST_HASH_SEED); // header hash

	// header hash check
    if (*headerHash != headHash)
    {
      throw(std::runtime_error(FSTERROR_COMP_HEADER));
    }

    // version check
	if (*version > FST_COMPRESS_VERSION)
	{
		throw(std::runtime_error(FSTERROR_COMP_FUTURE_VERSION));
	}

	// Source vector has correct length
	if (blockOffsets[nrOfBlocks] != blobLength)
	{
		throw(std::runtime_error(FSTERROR_COMP_SIZE));
	}

    // Create result blob
	IBlobContainer* blob_container = typeFactory->CreateBlobContainer(*vecLength);
    unsigned char* blob_data = blob_container->Data();

    // Determine required number of threads
    nrOfThreads = std::min(nrOfBlocks, nrOfThreads);

    // Determine number of blocks per (thread) batch
    const double batchFactor = static_cast<double>(nrOfBlocks) / nrOfThreads;

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
          const int fromBlock = static_cast<int>(batch * batchFactor + DOUBLE_DELTA); // start block
          const int toBlock = static_cast<int>((batch + 1) * batchFactor + DOUBLE_DELTA); // end block

          // iterate block range
          for (int block = fromBlock; block < toBlock; block++)
          {
            const unsigned long long blockStart = blockOffsets[block];
            const unsigned long long blockEnd = blockOffsets[block + 1];

            blockHashes[block] = XXH64(blobSource + blockStart, blockEnd - blockStart, FST_HASH_SEED);
          }
        }

#pragma omp single
        {
          const int fromBlock = static_cast<int>((nrOfThreads - 1) * batchFactor + DOUBLE_DELTA); // start block
          const int toBlock = static_cast<int>(nrOfThreads * batchFactor + DOUBLE_DELTA); // end block

          // iterate block range with full blocks
          for (int block = fromBlock; block < (toBlock - 1); block++)
          {
            const unsigned long long blockStart = blockOffsets[block];
            const unsigned long long blockEnd = blockOffsets[block + 1];

            blockHashes[block] = XXH64(blobSource + blockStart, blockEnd - blockStart, FST_HASH_SEED);
          }

          // last block
          const unsigned long long blockStart = blockOffsets[toBlock - 1];
          const unsigned long long blockEnd = blockOffsets[toBlock];

          blockHashes[toBlock - 1] = XXH64(blobSource + blockStart, blockEnd - blockStart, FST_HASH_SEED);
        }
      }

      const unsigned long long totHashes = XXH64(blockHashes, 8 * nrOfBlocks, FST_HASH_SEED);

      if (totHashes != *hashResult)
      {
        delete blob_container;
        throw(std::runtime_error(FSTERROR_COMP_DATA_HASH));
      }
    }

#pragma omp parallel num_threads(nrOfThreads)
    {
#pragma omp for schedule(static, 1) nowait
      for (int batch = 0; batch < (nrOfThreads - 1); batch++) // all but last batch
      {
        const int fromBlock = static_cast<int>(batch * batchFactor + DOUBLE_DELTA); // start block
        const int toBlock = static_cast<int>((batch + 1) * batchFactor + DOUBLE_DELTA); // end block

        // iterate block range
        for (int block = fromBlock; block < toBlock; block++)
        {
          const unsigned long long blockStart = blockOffsets[block];
          const unsigned long long blockEnd = blockOffsets[block + 1];

          const unsigned int errorCode = decompressor.Decompress(algorithm, reinterpret_cast<char*>(blob_data) + *blockSize * block,
            *blockSize, reinterpret_cast<const char*>(blobSource + blockStart), blockEnd - blockStart);

          if (errorCode != 0)
          {
            error = true;
          }
        }
      }

#pragma omp single
      {
        const int fromBlock = static_cast<int>((nrOfThreads - 1) * batchFactor + DOUBLE_DELTA); // start block
        const int toBlock = static_cast<int>(nrOfThreads * batchFactor + DOUBLE_DELTA); // end block

        // iterate block range with full blocks
        for (int block = fromBlock; block < (toBlock - 1); block++)
        {
          const unsigned long long blockStart = blockOffsets[block];
          const unsigned long long blockEnd = blockOffsets[block + 1];
          const unsigned int errorCode = decompressor.Decompress(algorithm, reinterpret_cast<char*>(blob_data) + *blockSize * block, *blockSize,
            reinterpret_cast<const char*>(blobSource + blockStart), blockEnd - blockStart);

          if (errorCode != 0)
          {
            error = true;
          }
        }

        // last block
        const int lastBlockSize = 1 + (*vecLength - 1) % *blockSize;
        const unsigned long long blockStart = blockOffsets[toBlock - 1];
        const unsigned long long blockEnd = blockOffsets[toBlock];
        const unsigned int errorCode = decompressor.Decompress(algorithm, reinterpret_cast<char*>(blob_data) + *blockSize * (toBlock - 1),
          lastBlockSize, reinterpret_cast<const char*>(blobSource + blockStart), blockEnd - blockStart);

        if (errorCode != 0)
        {
          error = true;
        }
      }
    }


    if (error)
    {
		delete blob_container;
		throw(std::runtime_error(FSTERROR_COMP_STREAM));
    }

    return blob_container;
  }
};


#endif  // FST_COMPRESSOR_H
