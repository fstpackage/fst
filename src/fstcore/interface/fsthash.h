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
	 * \brief Compress data with the 'LZ4' or 'ZSTD' compressor.
	 * \param blobSource source buffer to compress.
	 * \param blobLength Length of source buffer.
	 */
	unsigned int HashBlob(unsigned char* blobSource, unsigned long long blobLength) const
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

		unsigned int* blockHashes = new unsigned int[nrOfBlocks];

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
					blockHashes[block] = XXH32(&blobSource[block * blockSize], blockSize, FST_HASH_SEED);
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
					blockHashes[block] = XXH32(&blobSource[block * blockSize], blockSize, FST_HASH_SEED);
				}

				// Last block hash
				blockHashes[nextblockNr] = XXH32(&blobSource[nextblockNr * blockSize], lastBlockSize, FST_HASH_SEED);
			}

		}  // end parallel region and join all threads


		unsigned allBlockHash = XXH32(blockHashes, nrOfBlocks * 4, FST_HASH_SEED);
		delete[] blockHashes;

		return allBlockHash;
	}
};

#endif  // FST_HASH_H
