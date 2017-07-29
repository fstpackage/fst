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
#include <algorithm>
#include <cstring>

#include "compression/compressor.h"
#include "interface/fstdefines.h"
#include "interface/itypefactory.h"
#include "interface/openmphelper.h"


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
	COMPRESSION_ALGORITHM compAlgorithm;

public:

	FstCompressor(COMPRESSION_ALGORITHM compAlgorithm, unsigned int compressionLevel, ITypeFactory* typeFactory)
	{
		this->typeFactory = typeFactory;
		this->compAlgorithm = compAlgorithm;

		switch (compAlgorithm)
		{
			case COMPRESSION_ALGORITHM::ALGORITHM_LZ4:
			{
				compressor = new SingleCompressor(CompAlgo::LZ4, compressionLevel);
				break;
			}

			case COMPRESSION_ALGORITHM::ALGORITHM_ZSTD:
			{
				compressor = new SingleCompressor(CompAlgo::ZSTD, compressionLevel);
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
	IBlobContainer* CompressBlob(unsigned char* blobSource, unsigned long long blobLength, int nrOfCompressionBlocks = -1) const
	{
		int nrOfThreads = GetFstThreads();

		int prefNrOfBlocks = nrOfThreads * (1 + (PREV_NR_OF_BLOCKS - 1) / nrOfThreads);

		if (nrOfCompressionBlocks != -1)
		{
			prefNrOfBlocks = nrOfCompressionBlocks;
		}

		// block size to use for compression has a lower bound for compression efficiency
		unsigned long long blockSize = std::max(static_cast<unsigned long long>(BLOCKSIZE), 1 + (blobLength - 1) / prefNrOfBlocks);

		int nrOfBlocks = static_cast<int>(1 + (blobLength - 1) / blockSize);
		nrOfThreads = std::min(nrOfThreads, nrOfBlocks);

		unsigned int maxCompressSize = this->compressor->CompressBufferSize(blockSize);
		unsigned int lastBlockSize = blobLength % blockSize;

		unsigned long long bufSize = nrOfBlocks * maxCompressSize;
		float blocksPerThread = static_cast<float>(nrOfBlocks / nrOfThreads);

		// Compressed sizes
		unsigned long long* compSizes = new unsigned long long[nrOfBlocks];
		unsigned long long* compBatchSizes = new unsigned long long[nrOfThreads];

		// We need nrOfBlocks buffers
		unsigned char* calcBuffer = new unsigned char[bufSize];

#pragma omp parallel num_threads(nrOfThreads) shared(compSizes,nrOfThreads,blocksPerThread,maxCompressSize,lastBlockSize,compBatchSizes)
		{
#pragma omp for schedule(static, 1) nowait
			for (int blockBatch = 0; blockBatch < (nrOfThreads - 1); blockBatch++)  // all but last batch
			{
				float blockOffset = blockBatch * blocksPerThread;
				int blockNr = static_cast<int>(0.00001 + blockOffset);
				int nextblockNr = static_cast<int>(blocksPerThread + 0.00001 + blockOffset);
				CompAlgo compAlgo;

				unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr;  // buffer for compression results of current thread

				// inner loop is executed by current thread
				unsigned long long bufPos = 0;
				for (int block = blockNr; block < nextblockNr; block++)
				{
					int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
						reinterpret_cast<char*>(&blobSource[block * blockSize]), blockSize, compAlgo);
					compSizes[block] = static_cast<unsigned long long>(compSize);
					bufPos += compSize;
				}

				compBatchSizes[blockBatch] = bufPos;
			}

#pragma omp single
			{
				int blockNr = static_cast<int>(0.00001 + (nrOfThreads - 1) * blocksPerThread);
				int nextblockNr = static_cast<int>(0.00001 + (nrOfThreads * blocksPerThread)) - 1;  // exclude last block
				CompAlgo compAlgo;

				unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr;  // buffer for compression results of current thread

				// inner loop is executed by current thread
				unsigned long long bufPos = 0;
				for (int block = blockNr; block < nextblockNr; block++)
				{
					int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
						reinterpret_cast<char*>(&blobSource[block * blockSize]), blockSize, compAlgo);
					compSizes[block] = static_cast<unsigned long long>(compSize);
					bufPos += compSize;
				}

				// last block
				int compSize = this->compressor->Compress(reinterpret_cast<char*>(threadBuf + bufPos), maxCompressSize,
					reinterpret_cast<char*>(&blobSource[nextblockNr * blockSize]), lastBlockSize, compAlgo);
				compSizes[nextblockNr] = static_cast<unsigned long long>(compSize);

				compBatchSizes[nrOfThreads - 1] = bufPos + compSize;;
			}

		}  // end parallel region and join all threads

		unsigned long long totCompSize = 0;
		for (int blockBatch = 0; blockBatch < nrOfThreads; blockBatch++)
		{
			totCompSize += compBatchSizes[blockBatch];
		}

		// In memory compression format:
		// Size           | Type               | Description
		// 4              | unsigned int       | fst marker
		// 4              | int                | COMPRESSION_ALGORITHM
		// 8              | unsigned long long | vecLength
		// 8 * nrOfBlocks | unsigned long long | block offset
		//                | unsigned char      | compressed data

		unsigned long long headerSize = 8 * (nrOfBlocks + 2);
		IBlobContainer* blobContainer = typeFactory->CreateBlobContainer(headerSize + totCompSize);
		unsigned char* blobData = blobContainer->Data();

		unsigned int* fstMarker = reinterpret_cast<unsigned int*>(blobData);
		int* algo = reinterpret_cast<int*>(blobData + 4);
		unsigned long long* vecLength = reinterpret_cast<unsigned long long*>(blobData + 8);
		unsigned long long* blockOffsets = reinterpret_cast<unsigned long long*>(blobData + 16);

		*fstMarker = FST_MEMORY_MARKER;
		*algo = compAlgorithm;
		*vecLength = blobLength;

		// calculate offsets for memcpy
		unsigned long long dataOffset = headerSize;
		unsigned long long* dataOffsets = new unsigned long long[nrOfThreads];
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
			unsigned char* threadBuf = calcBuffer + maxCompressSize * blockNr;  // buffer for compression results of current thread
			std::memcpy(blobData + dataOffsets[blockBatch], threadBuf, compBatchSizes[blockBatch]);
		}  // end parallel region

		delete[] compBatchSizes;
		delete[] calcBuffer;
		delete[] dataOffsets;

		unsigned long long blockOffset = headerSize;
		for (int block = 0; block < nrOfBlocks; block++)
		{
			blockOffset += compSizes[block];
			blockOffsets[block] = blockOffset;
		}

		delete[] compSizes;

		return blobContainer;
	}

	// IBlobContainer* DecompressBlob(unsigned char* blobSource, unsigned long long blobLength) const
	// {
	//
	// }

};


#endif  // FST_STORE_H
