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


#ifndef FST_COMPRESSOR_H
#define FST_COMPRESSOR_H


#include <stdlib.h>

#include "compression/compressor.h"
#include "interface/fstdefines.h"
#include "interface/itypefactory.h"
#include "interface/openmphelper.h"
#include <algorithm>


enum COMPRESSION_ALGORITHM
{
	ALGORITHM_NONE = 0,
	ALGORITHM_LZ4,
	ALGORITHM_ZSTD
};


class FstCompressor
{
	Compressor* compressor = nullptr;
	ITypeFactory* typeFactory;

public:

	FstCompressor(COMPRESSION_ALGORITHM compAlgorithm, unsigned int compressionLevel, ITypeFactory* typeFactory)
	{
		this->typeFactory = typeFactory;

		switch (compAlgorithm)
		{
			case COMPRESSION_ALGORITHM::ALGORITHM_LZ4:
			{
				compressor = new SingleCompressor(CompAlgo::LZ4, compressionLevel);
				break;
			}

			case COMPRESSION_ALGORITHM::ALGORITHM_ZSTD:
			{
				compressor = new SingleCompressor(CompAlgo::LZ4, compressionLevel);
				break;
			}

			default:
			{
				compressor = new SingleCompressor(CompAlgo::LZ4, compressionLevel);
				break;
			}
		}
	}


	/**
	 * \brief Compress data with the 'LZ4' or 'ZSTD' compressor.
	 * \param blobSource source buffer to compress.
	 * \param blobLength Length of source buffer.
	 */
	IBlobContainer* CompressBlob(unsigned char* blobSource, unsigned long long blobLength) const
	{
		int nrOfBlocks = 1 + ((blobLength - 1) / BLOCKSIZE);
		unsigned long long maxCompressSize = this->compressor->CompressBufferSize(BLOCKSIZE);
		unsigned long long lastBlockSize = blobLength % BLOCKSIZE;

		int nrOfThreads = std::max(1, std::min(GetFstThreads(), nrOfBlocks));  // at minimum a single block per thread
		unsigned long long bufSize = nrOfBlocks * maxCompressSize;
		float blocksPerThread = static_cast<float>(nrOfBlocks - 1) / nrOfThreads;  // skip last block here

		// Compressed sizes
		int* compSizes = new int[nrOfBlocks];

		// We need nrOfBlocks buffers
		unsigned char* calcBuffer = new unsigned char[bufSize];

#pragma omp parallel num_threads(nrOfThreads) shared(compSizes, nrOfThreads,blocksPerThread,maxCompressSize)
		{
#pragma omp for schedule(static, 1) nowait
			for (int blockBatch = 0; blockBatch < nrOfThreads; blockBatch++)
			{
				int blockNr = static_cast<int>(0.00001 + (blockBatch * blocksPerThread));
				int nextblockNr = static_cast<int>(blocksPerThread + 0.00001 + (blockBatch * blocksPerThread));
				CompAlgo compAlgo;

				unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr;  // buffer for compression results of current thread

				// inner loop is executed by current thread
				unsigned long long bufPos = 0;
				for (int block = blockNr; block < nextblockNr; block++)
				{
					int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
						reinterpret_cast<char*>(&blobSource[block * BLOCKSIZE]), BLOCKSIZE, compAlgo);
					compSizes[block] = compSize;
					bufPos += compSize;
				}
			}

			// Compress last block with first thread that finishes
#pragma omp single
			{
				int lastBlockNr = nrOfBlocks - 1;
				unsigned char* threadBuf = calcBuffer + maxCompressSize * lastBlockNr;
				CompAlgo compAlgo;
				int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf), maxCompressSize,
					reinterpret_cast<char*>(&blobSource[lastBlockNr * BLOCKSIZE]), BLOCKSIZE, compAlgo);
				compSizes[lastBlockNr] = compSize;
			}
		}  // end parallel region and join all threads

		// Get total compressed size
		unsigned long long outSize = 0;
		for (int block = 0; block < nrOfBlocks; block++)
		{
			outSize += compSizes[block];
		}

		IBlobContainer* blobContainer = typeFactory->CreateBlobContainer(outSize);
		unsigned char* blobData = blobContainer->Data();

		for (int blockBatch = 0; blockBatch < nrOfThreads; blockBatch++)
		{
			// memcpy to outVec here!
		}
	}
};


#endif  // FST_STORE_H
