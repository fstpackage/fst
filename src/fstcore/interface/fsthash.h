/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  BSD 3-Clause (https://opensource.org/licenses/BSD-3-Clause)

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.

  3. Neither the name of the copyright holder nor the names of its
     contributors may be used to endorse or promote products derived from
     this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact the author at :
  - fstlib source repository : https://github.com/fstPackage/fstlib
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

	unsigned int HashBlob(unsigned char* blobSource, unsigned long long blobLength) const
	{
		return HashBlob(blobSource, blobLength, FST_HASH_SEED);
	}

	/**
	 * \brief Compress data with the 'LZ4' or 'ZSTD' compressor.
	 * \param blobSource source buffer to compress.
	 * \param blobLength Length of source buffer.
	 * \param seed Seed for hashing
	 */
	unsigned int HashBlob(unsigned char* blobSource, unsigned long long blobLength, unsigned int seed) const
	{
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


		unsigned long long allBlockHash = XXH64(blockHashes, nrOfBlocks * 8, seed);
		delete[] blockHashes;

		return static_cast<unsigned int>((allBlockHash >> 32) & 0xffffffff);
	}
};

#endif  // FST_HASH_H
