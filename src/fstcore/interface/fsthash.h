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


#ifndef FST_HASH_H
#define FST_HASH_H


#include <stdlib.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>

#include "interface/fstdefines.h"
#include "interface/itypefactory.h"
#include "interface/openmphelper.h"

#include "ZSTD/common/xxhash.h"


class FstHasher
{
public:

	FstHasher()
	{
	}

  /**
	 * \brief Calculate 64-bit xxHash of an arbitrary length input vector
	 * \param blobSource input buffer for hash calculation
	 * \param blobLength length of the input buffer in bytes
	 * \param blockHash if true, use a multi-threaded blocked hash format (hash of hashes)
	 * \return hash value of the input buffer
	 */
	unsigned long long HashBlob(unsigned char* blobSource, unsigned long long blobLength, bool blockHash = true) const
	{
		return HashBlobSeed(blobSource, blobLength, FST_HASH_SEED, blockHash);
	}

	/**
  * \brief Calculate 64-bit xxHash of an arbitrary length input vector
  * \param blobSource input buffer for hash calculation
  * \param blobLength length of the input buffer in bytes
  * \param seed Seed for hashing
  * \param blockHash if true, use a multi-threaded blocked hash format (hash of hashes)
	* \return hash value of the input buffer
   */
	unsigned long long HashBlobSeed(unsigned char* blobSource, unsigned long long blobLength, unsigned long long seed, bool blockHash = true) const
	{
    if (!blockHash)
    {
      return XXH64(blobSource, blobLength, seed);
    }

		int nrOfThreads = GetFstThreads();

		// Check for empty source
		if (blobLength == 0)
		{
			throw(std::runtime_error("Source contains no data."));
		}

		// block size to use for hashing has a lower bound for compression efficiency
		unsigned long long minBlock = std::max(static_cast<unsigned long long>(HASH_SIZE),
			1 + (blobLength - 1) / PREV_NR_OF_BLOCKS);

		// And a higher bound for hasher compatability
		unsigned int blockSize = static_cast<unsigned int>(std::min(minBlock, static_cast<unsigned long long>(INT_MAX)));

		int nrOfBlocks = static_cast<int>(1 + (blobLength - 1) / blockSize);
		nrOfThreads = std::min(nrOfThreads, nrOfBlocks);

		unsigned int lastBlockSize = 1 + (blobLength - 1) % blockSize;
		float blocksPerThread = static_cast<float>(nrOfBlocks) / nrOfThreads;

		unsigned long long* blockHashes = new unsigned long long[nrOfBlocks];

#pragma omp parallel num_threads(nrOfThreads)
		{
#pragma omp for schedule(static, 1) nowait
			for (int blockBatch = 0; blockBatch < (nrOfThreads - 1); blockBatch++)  // all but last batch
			{
				float blockOffset = blockBatch * blocksPerThread;
				int blockNr = static_cast<int>(0.00001 + blockOffset);
				int nextblockNr = static_cast<int>(blocksPerThread + 0.00001 + blockOffset);

				// inner loop is executed by current thread
				for (int block = blockNr; block < nextblockNr; block++)
				{
					// Block hash
					blockHashes[block] = XXH64(&blobSource[block * blockSize], blockSize, seed);
				}
			}

#pragma omp single
			{
				int blockNr = static_cast<int>(0.00001 + (nrOfThreads - 1) * blocksPerThread);
				int nextblockNr = static_cast<int>(0.00001 + (nrOfThreads * blocksPerThread)) - 1;  // exclude last block

				// inner loop is executed by current thread
				for (int block = blockNr; block < nextblockNr; block++)
				{
					// Block hash
					blockHashes[block] = XXH64(&blobSource[block * blockSize], blockSize, seed);
				}

				// Last block hash
				blockHashes[nextblockNr] = XXH64(&blobSource[nextblockNr * blockSize], lastBlockSize, seed);
			}

		}  // end parallel region and join all threads

    unsigned long long allBlockHash = blockHashes[0];

    // combine multiple hashes
    if (nrOfBlocks > 1)
    {
      allBlockHash = XXH64(blockHashes, nrOfBlocks * 8, seed);
    }
		delete[] blockHashes;

		//return static_cast<unsigned int>((allBlockHash >> 32) & 0xffffffff);
    return allBlockHash;
  }
};

#endif  // FST_HASH_H
