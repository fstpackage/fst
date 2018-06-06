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

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <iostream>
#include <fstream>

#include <compression/compression.h>
#include <interface/fstdefines.h>

// #include <unordered_map>
// #include <boost/unordered_map.hpp>

#include <zstd.h>

#define LZ4_DISABLE_DEPRECATE_WARNINGS  // required for Clang++6.0 compiler error
#include <lz4.h>


using namespace std;


size_t MAX_compressBound(size_t srcSize)
{
	return max(ZSTD_compressBound(srcSize), LZ4_COMPRESSBOUND(srcSize));
}

// The size of outVec is expected to be 2 times nrOfDoubles
void ShuffleReal(double* inVec, double* outVec, int nrOfDoubles)
{
  int blockLength = nrOfDoubles / 8;

  unsigned long long* vecIn  = (unsigned long long*) inVec;
  unsigned long long* vecOut = (unsigned long long*) outVec;

  // Bit masks for long ints
  unsigned long long byte0 = 255;
  unsigned long long byte1 = byte0 << 8;
  unsigned long long byte2 = byte1 << 8;
  unsigned long long byte3 = byte2 << 8;
  unsigned long long byte4 = byte3 << 8;
  unsigned long long byte5 = byte4 << 8;
  unsigned long long byte6 = byte5 << 8;
  unsigned long long byte7 = byte6 << 8;

  int offset = -1;
  int blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine most significant byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 56) & byte0) |
      ((vecIn[blockIndex + 6] >> 48) & byte1) |
      ((vecIn[blockIndex + 5] >> 40) & byte2) |
      ((vecIn[blockIndex + 4] >> 32) & byte3) |
      ((vecIn[blockIndex + 3] >> 24) & byte4) |
      ((vecIn[blockIndex + 2] >> 16) & byte5) |
      ((vecIn[blockIndex + 1] >> 8 ) & byte6) |
      (vecIn[blockIndex    ]        & byte7);

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 7th byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 48) & byte0) |
      ((vecIn[blockIndex + 6] >> 40) & byte1) |
      ((vecIn[blockIndex + 5] >> 32) & byte2) |
      ((vecIn[blockIndex + 4] >> 24) & byte3) |
      ((vecIn[blockIndex + 3] >> 16) & byte4) |
      ((vecIn[blockIndex + 2] >> 8 ) & byte5) |
      ( vecIn[blockIndex + 1]        & byte6) |
      ((vecIn[blockIndex    ] << 8 ) & byte7);
    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 6th byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 40) & byte0) |
      ((vecIn[blockIndex + 6] >> 32) & byte1) |
      ((vecIn[blockIndex + 5] >> 24) & byte2) |
      ((vecIn[blockIndex + 4] >> 16) & byte3) |
      ((vecIn[blockIndex + 3] >> 8 ) & byte4) |
      ( vecIn[blockIndex + 2]        & byte5) |
      ((vecIn[blockIndex + 1] << 8 ) & byte6) |
      ((vecIn[blockIndex    ] << 16) & byte7);

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 5th byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 32) & byte0) |
      ((vecIn[blockIndex + 6] >> 24) & byte1) |
      ((vecIn[blockIndex + 5] >> 16) & byte2) |
      ((vecIn[blockIndex + 4] >> 8 ) & byte3) |
      ( vecIn[blockIndex + 3]        & byte4) |
      ((vecIn[blockIndex + 2] << 8 ) & byte5) |
      ((vecIn[blockIndex + 1] << 16) & byte6) |
      ((vecIn[blockIndex    ] << 24) & byte7);

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 4th byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 24) & byte0) |
      ((vecIn[blockIndex + 6] >> 16) & byte1) |
      ((vecIn[blockIndex + 5] >> 8 ) & byte2) |
      ( vecIn[blockIndex + 4]        & byte3) |
      ((vecIn[blockIndex + 3] << 8 ) & byte4) |
      ((vecIn[blockIndex + 2] << 16) & byte5) |
      ((vecIn[blockIndex + 1] << 24) & byte6) |
      ((vecIn[blockIndex    ] << 32) & byte7);

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 3th byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 16) & byte0) |
      ((vecIn[blockIndex + 6] >> 8 ) & byte1) |
      ( vecIn[blockIndex + 5]        & byte2) |
      ((vecIn[blockIndex + 4] << 8 ) & byte3) |
      ((vecIn[blockIndex + 3] << 16) & byte4) |
      ((vecIn[blockIndex + 2] << 24) & byte5) |
      ((vecIn[blockIndex + 1] << 32) & byte6) |
      ((vecIn[blockIndex    ] << 40) & byte7);

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 2nd byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 7] >> 8 ) & byte0) |
      ( vecIn[blockIndex + 6]        & byte1) |
      ((vecIn[blockIndex + 5] << 8 ) & byte2) |
      ((vecIn[blockIndex + 4] << 16) & byte3) |
      ((vecIn[blockIndex + 3] << 24) & byte4) |
      ((vecIn[blockIndex + 2] << 32) & byte5) |
      ((vecIn[blockIndex + 1] << 40) & byte6) |
      ((vecIn[blockIndex    ] << 48) & byte7);

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 1st byte
    vecOut[++offset] =
      ( vecIn[blockIndex + 7]        & byte0) |
      ((vecIn[blockIndex + 6] << 8 ) & byte1) |
      ((vecIn[blockIndex + 5] << 16) & byte2) |
      ((vecIn[blockIndex + 4] << 24) & byte3) |
      ((vecIn[blockIndex + 3] << 32) & byte4) |
      ((vecIn[blockIndex + 2] << 40) & byte5) |
      ((vecIn[blockIndex + 1] << 48) & byte6) |
      ((vecIn[blockIndex    ] << 56) & byte7);

    blockIndex += 8;
  }

  // Copy remaining doubles unmodified
  int remain = nrOfDoubles % 8;
  int* inVecInt = (int*) &inVec[blockLength * 8];

  int outPos = blockLength * 8;

  memcpy(&outVec[outPos], inVecInt, remain * 8);
}


void DeshuffleReal(double* inVec, double* outVec, int nrOfDoubles)
{
  int blockLength = nrOfDoubles / 8;

  unsigned long long* vecInReal  = (unsigned long long*) inVec;
  unsigned long long* vecOutReal = (unsigned long long*) outVec;

  // Bit masks for long ints
  unsigned long long byte0 = 255;
  unsigned long long byte1 = byte0 << 8;
  unsigned long long byte2 = byte1 << 8;
  unsigned long long byte3 = byte2 << 8;
  unsigned long long byte4 = byte3 << 8;
  unsigned long long byte5 = byte4 << 8;
  unsigned long long byte6 = byte5 << 8;
  unsigned long long byte7 = byte6 << 8;

  int offset = -1;
  int blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine most significant byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] = (compVal << 56) & byte7;
    vecOutReal[blockIndex + 6] = (compVal << 48) & byte7;
    vecOutReal[blockIndex + 5] = (compVal << 40) & byte7;
    vecOutReal[blockIndex + 4] = (compVal << 32) & byte7;
    vecOutReal[blockIndex + 3] = (compVal << 24) & byte7;
    vecOutReal[blockIndex + 2] = (compVal << 16) & byte7;
    vecOutReal[blockIndex + 1] = (compVal << 8 ) & byte7;
    vecOutReal[blockIndex    ] = (compVal      ) & byte7;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 7th byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal << 48) & byte6;
    vecOutReal[blockIndex + 6] |= (compVal << 40) & byte6;
    vecOutReal[blockIndex + 5] |= (compVal << 32) & byte6;
    vecOutReal[blockIndex + 4] |= (compVal << 24) & byte6;
    vecOutReal[blockIndex + 3] |= (compVal << 16) & byte6;
    vecOutReal[blockIndex + 2] |= (compVal << 8 ) & byte6;
    vecOutReal[blockIndex + 1] |= (compVal      ) & byte6;
    vecOutReal[blockIndex    ] |= (compVal >> 8 ) & byte6;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 6th byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal << 40) & byte5;
    vecOutReal[blockIndex + 6] |= (compVal << 32) & byte5;
    vecOutReal[blockIndex + 5] |= (compVal << 24) & byte5;
    vecOutReal[blockIndex + 4] |= (compVal << 16) & byte5;
    vecOutReal[blockIndex + 3] |= (compVal << 8 ) & byte5;
    vecOutReal[blockIndex + 2] |= (compVal      ) & byte5;
    vecOutReal[blockIndex + 1] |= (compVal >> 8 ) & byte5;
    vecOutReal[blockIndex    ] |= (compVal >> 16) & byte5;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 5th byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal << 32) & byte4;
    vecOutReal[blockIndex + 6] |= (compVal << 24) & byte4;
    vecOutReal[blockIndex + 5] |= (compVal << 16) & byte4;
    vecOutReal[blockIndex + 4] |= (compVal << 8 ) & byte4;
    vecOutReal[blockIndex + 3] |= (compVal      ) & byte4;
    vecOutReal[blockIndex + 2] |= (compVal >> 8 ) & byte4;
    vecOutReal[blockIndex + 1] |= (compVal >> 16) & byte4;
    vecOutReal[blockIndex    ] |= (compVal >> 24) & byte4;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 4th byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal << 24) & byte3;
    vecOutReal[blockIndex + 6] |= (compVal << 16) & byte3;
    vecOutReal[blockIndex + 5] |= (compVal << 8 ) & byte3;
    vecOutReal[blockIndex + 4] |= (compVal      ) & byte3;
    vecOutReal[blockIndex + 3] |= (compVal >> 8 ) & byte3;
    vecOutReal[blockIndex + 2] |= (compVal >> 16) & byte3;
    vecOutReal[blockIndex + 1] |= (compVal >> 24) & byte3;
    vecOutReal[blockIndex    ] |= (compVal >> 32) & byte3;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 4th byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal << 16) & byte2;
    vecOutReal[blockIndex + 6] |= (compVal << 8 ) & byte2;
    vecOutReal[blockIndex + 5] |= (compVal      ) & byte2;
    vecOutReal[blockIndex + 4] |= (compVal >> 8 ) & byte2;
    vecOutReal[blockIndex + 3] |= (compVal >> 16) & byte2;
    vecOutReal[blockIndex + 2] |= (compVal >> 24) & byte2;
    vecOutReal[blockIndex + 1] |= (compVal >> 32) & byte2;
    vecOutReal[blockIndex    ] |= (compVal >> 40) & byte2;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 2nd byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal << 8 ) & byte1;
    vecOutReal[blockIndex + 6] |= (compVal      ) & byte1;
    vecOutReal[blockIndex + 5] |= (compVal >> 8 ) & byte1;
    vecOutReal[blockIndex + 4] |= (compVal >> 16) & byte1;
    vecOutReal[blockIndex + 3] |= (compVal >> 24) & byte1;
    vecOutReal[blockIndex + 2] |= (compVal >> 32) & byte1;
    vecOutReal[blockIndex + 1] |= (compVal >> 40) & byte1;
    vecOutReal[blockIndex    ] |= (compVal >> 48) & byte1;

    blockIndex += 8;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine least significant byte
    unsigned long long compVal = vecInReal[++offset];

    vecOutReal[blockIndex + 7] |= (compVal      ) & byte0;
    vecOutReal[blockIndex + 6] |= (compVal >> 8 ) & byte0;
    vecOutReal[blockIndex + 5] |= (compVal >> 16) & byte0;
    vecOutReal[blockIndex + 4] |= (compVal >> 24) & byte0;
    vecOutReal[blockIndex + 3] |= (compVal >> 32) & byte0;
    vecOutReal[blockIndex + 2] |= (compVal >> 40) & byte0;
    vecOutReal[blockIndex + 1] |= (compVal >> 48) & byte0;
    vecOutReal[blockIndex    ] |= (compVal >> 56) & byte0;

    blockIndex += 8;
  }

  // Copy remaining doubles unmodified
  int remain = nrOfDoubles % 8;
  int pos = blockLength * 8;

  memcpy(&outVec[pos], &inVec[pos], remain * 8);
}


// The size of outVec must be equal to nrOfInts
void ShuffleInt2(int* inVec, int* outVec, int nrOfInts)
{
  // Determine block length in number of longs
  int blockLength = nrOfInts / 8;

  unsigned long long* vecIn  = (unsigned long long*) inVec;
  unsigned long long* vecOut = (unsigned long long*) outVec;

  // Bit masks for 2 integers
  unsigned long long byte0 = (255LL << 32) | 255;
  unsigned long long byte1 = byte0 << 8;
  unsigned long long byte2 = byte0 << 16;
  unsigned long long byte3 = byte0 << 24;

  int offset = -1;
  int blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine most significant byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 3] >> 24) & byte0) |
      ((vecIn[blockIndex + 2] >> 16) & byte1) |
      ((vecIn[blockIndex + 1] >> 8 ) & byte2) |
      ( vecIn[blockIndex    ]        & byte3);

    blockIndex += 4;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 3th byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 3] >> 16) & byte0) |
      ((vecIn[blockIndex + 2] >> 8 ) & byte1) |
      ( vecIn[blockIndex + 1]        & byte2) |
      ((vecIn[blockIndex    ]  << 8) & byte3);

    blockIndex += 4;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 2nd byte
    vecOut[++offset] =
      ((vecIn[blockIndex + 3] >> 8 ) & byte0) |
      ( vecIn[blockIndex + 2]        & byte1) |
      ((vecIn[blockIndex + 1] << 8 ) & byte2) |
      ((vecIn[blockIndex    ] << 16) & byte3);

    blockIndex += 4;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 5th byte
    vecOut[++offset] =
      ( vecIn[blockIndex + 3]        & byte0) |
      ((vecIn[blockIndex + 2] << 8 ) & byte1) |
      ((vecIn[blockIndex + 1] << 16) & byte2) |
      ((vecIn[blockIndex    ] << 24) & byte3);

    blockIndex += 4;
  }

  // Copy remaining integers unmodified
  int remain = nrOfInts % 8;
  int pos = blockLength * 8;

  memcpy(&outVec[pos], &inVec[pos], remain * 4);
}


void DeshuffleInt2(int* inVec, int* outVec, int nrOfInts)
{
  int blockLength = nrOfInts / 8;

  unsigned long long* vecInLong  = (unsigned long long*) inVec;
  unsigned long long* vecOutLong = (unsigned long long*) outVec;

  // Bit masks for 2 integers
  unsigned long long byte0 = (255LL << 32) | 255;
  unsigned long long byte1 = byte0 << 8;
  unsigned long long byte2 = byte0 << 16;
  unsigned long long byte3 = byte0 << 24;

  int offset = -1;
  int blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine most significant byte
    unsigned long long compVal = vecInLong[++offset];

    vecOutLong[blockIndex + 3] = (compVal << 24) & byte3;
    vecOutLong[blockIndex + 2] = (compVal << 16) & byte3;
    vecOutLong[blockIndex + 1] = (compVal << 8 ) & byte3;
    vecOutLong[blockIndex    ] = (compVal      ) & byte3;

    blockIndex += 4;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 3th byte
    unsigned long long compVal = vecInLong[++offset];

    vecOutLong[blockIndex + 3] |= (compVal << 16) & byte2;
    vecOutLong[blockIndex + 2] |= (compVal << 8 ) & byte2;
    vecOutLong[blockIndex + 1] |= (compVal      ) & byte2;
    vecOutLong[blockIndex    ] |= (compVal >> 8 ) & byte2;

    blockIndex += 4;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine 2nd byte
    unsigned long long compVal = vecInLong[++offset];

    vecOutLong[blockIndex + 3] |= (compVal << 8 ) & byte1;
    vecOutLong[blockIndex + 2] |= (compVal      ) & byte1;
    vecOutLong[blockIndex + 1] |= (compVal >> 8 ) & byte1;
    vecOutLong[blockIndex    ] |= (compVal >> 16) & byte1;

    blockIndex += 4;
  }

  blockIndex = 0;
  for (int i = 0; i < blockLength; ++i)
  {
    // Combine least significant byte
    unsigned long long compVal = vecInLong[++offset];

    vecOutLong[blockIndex + 3] |= (compVal      ) & byte0;
    vecOutLong[blockIndex + 2] |= (compVal >> 8 ) & byte0;
    vecOutLong[blockIndex + 1] |= (compVal >> 16) & byte0;
    vecOutLong[blockIndex    ] |= (compVal >> 24) & byte0;

    blockIndex += 4;
  }

  // Copy remaining integers unmodified
  int remain = nrOfInts % 8;
  int pos = blockLength * 8;

  memcpy(&outVec[pos], &inVec[pos], remain * 4);
}


// The first nrOfDiscard decompressed logicals are discarded. Parameter nrOfLogicals includes these discarded values,
// so nrOfLogicals must be equal or larger than nrOfDiscard.
void LogicDecompr64(char* logicalVec, const unsigned long long* compBuf, int nrOfLogicals, int nrOfDiscard)
{
  // Define filters
  unsigned long long BIT0 = (1LL << 32) | 1LL;
  unsigned long long BIT31 = BIT0 << 31;
  unsigned long long BIT = BIT0 | BIT31;

  // Determine logical offset
  if (nrOfDiscard > 0)
  {
    int partDiscard = nrOfDiscard % 32;

    // Discard complete integers from the compressed buffer
    if (partDiscard == 0)
    {
      int skipLongs = nrOfDiscard / 32;
      LogicDecompr64(logicalVec, &compBuf[skipLongs], nrOfLogicals - nrOfDiscard, 0);

      return;
    }

    // Process compressed integers partially
    int skipLongs = nrOfDiscard / 32;
    unsigned long long compVal = compBuf[skipLongs];  // compression buffer value with partial logicals

    // Decompress full value
    unsigned long long logics[16];
    logics[15] = (compVal << 15 & BIT31) | (compVal >> 15 & BIT0);
    logics[14] = (compVal << 14 & BIT31) | (compVal >> 14 & BIT0);
    logics[13] = (compVal << 13 & BIT31) | (compVal >> 13 & BIT0);
    logics[12] = (compVal << 12 & BIT31) | (compVal >> 12 & BIT0);
    logics[11] = (compVal << 11 & BIT31) | (compVal >> 11 & BIT0);
    logics[10] = (compVal << 10 & BIT31) | (compVal >> 10 & BIT0);
    logics[9 ] = (compVal << 9  & BIT31) | (compVal >> 9  & BIT0);
    logics[8 ] = (compVal << 8  & BIT31) | (compVal >> 8  & BIT0);
    logics[7 ] = (compVal << 7  & BIT31) | (compVal >> 7  & BIT0);
    logics[6 ] = (compVal << 6  & BIT31) | (compVal >> 6  & BIT0);
    logics[5 ] = (compVal << 5  & BIT31) | (compVal >> 5  & BIT0);
    logics[4 ] = (compVal << 4  & BIT31) | (compVal >> 4  & BIT0);
    logics[3 ] = (compVal << 3  & BIT31) | (compVal >> 3  & BIT0);
    logics[2 ] = (compVal << 2  & BIT31) | (compVal >> 2  & BIT0);
    logics[1 ] = (compVal << 1  & BIT31) | (compVal >> 1  & BIT0);
    logics[0 ] = compVal & BIT;

    int* logicsInt = (int*) logics;  // 32 logicals
    int logicalsLeft = nrOfLogicals - nrOfDiscard;

    int partLogicalsLeft = min(logicalsLeft, 32 - partDiscard);

    memcpy(logicalVec, &logicsInt[partDiscard], partLogicalsLeft * sizeof(int));

    logicalsLeft -= partLogicalsLeft;  // substract uncompressed logicals

    if (logicalsLeft <= 0)
    {
      return;
    }

    LogicDecompr64(&logicalVec[partLogicalsLeft], &compBuf[skipLongs + 1], logicalsLeft, 0);

    return;
  }

  // No logicals are discarded

  unsigned long long* logicals = (unsigned long long*) logicalVec;
  unsigned long long* compress = (unsigned long long*) compBuf;
  int nrOfLongs = nrOfLogicals / 32;

  // Compress in cycles of 32 logicals
  for (int i = 0; i < nrOfLongs; ++i)
  {
    unsigned long long* logics = &logicals[16 * i];
    unsigned long long compVal = compress[i];

    logics[15] = (compVal << 15 & BIT31) | (compVal >> 15 & BIT0);
    logics[14] = (compVal << 14 & BIT31) | (compVal >> 14 & BIT0);
    logics[13] = (compVal << 13 & BIT31) | (compVal >> 13 & BIT0);
    logics[12] = (compVal << 12 & BIT31) | (compVal >> 12 & BIT0);
    logics[11] = (compVal << 11 & BIT31) | (compVal >> 11 & BIT0);
    logics[10] = (compVal << 10 & BIT31) | (compVal >> 10 & BIT0);
    logics[9 ] = (compVal << 9  & BIT31) | (compVal >> 9  & BIT0);
    logics[8 ] = (compVal << 8  & BIT31) | (compVal >> 8  & BIT0);
    logics[7 ] = (compVal << 7  & BIT31) | (compVal >> 7  & BIT0);
    logics[6 ] = (compVal << 6  & BIT31) | (compVal >> 6  & BIT0);
    logics[5 ] = (compVal << 5  & BIT31) | (compVal >> 5  & BIT0);
    logics[4 ] = (compVal << 4  & BIT31) | (compVal >> 4  & BIT0);
    logics[3 ] = (compVal << 3  & BIT31) | (compVal >> 3  & BIT0);
    logics[2 ] = (compVal << 2  & BIT31) | (compVal >> 2  & BIT0);
    logics[1 ] = (compVal << 1  & BIT31) | (compVal >> 1  & BIT0);
    logics[0 ] = compVal & BIT;
  }

  // Process remainder
  int remain = nrOfLogicals % 32;

  if (remain == 0)
  {
    return;
  }


  // Compute remaining logicals
  unsigned long long compVal = compress[nrOfLongs];
  int nrOfRemainLongs = 1 + (remain - 1) / 2;  // per 2 logicals
  unsigned long long remainLogics[16];  // at maximum nrOfRemainLongs equals 16

  for (int remainNr = 0; remainNr < nrOfRemainLongs; ++remainNr)
  {
    remainLogics[remainNr] = (compVal << remainNr  & BIT31) | (compVal >> remainNr  & BIT0);
  }

  // Copy logicals to output buffer
  unsigned long long* logics = &logicals[16 * nrOfLongs];
  memcpy(logics, remainLogics, remain * sizeof(int));
}


// Compression buffer should be at least 1 + (nrOfLogicals - 1) / 256 elements (long ints) in length (factor 32)
void LogicCompr64(const char* logicalVec, unsigned long long* compress, int nrOfLogicals)
{
  const unsigned long long* logicals = (const unsigned long long*) logicalVec;
  int nrOfLongs = nrOfLogicals / 32;  // number of full longs

  // Define filters
  // TODO: define these as constants
  unsigned long long BIT = (1LL << 32) | 1LL;
  unsigned long long BIT0  = (BIT << 16) | (BIT << 15);
  unsigned long long BIT1  = (BIT << 17) | (BIT << 14);
  unsigned long long BIT2  = (BIT << 18) | (BIT << 13);
  unsigned long long BIT3  = (BIT << 19) | (BIT << 12);
  unsigned long long BIT4  = (BIT << 20) | (BIT << 11);
  unsigned long long BIT5  = (BIT << 21) | (BIT << 10);
  unsigned long long BIT6  = (BIT << 22) | (BIT << 9 );
  unsigned long long BIT7  = (BIT << 23) | (BIT << 8 );
  unsigned long long BIT8  = (BIT << 24) | (BIT << 7 );
  unsigned long long BIT9  = (BIT << 25) | (BIT << 6 );
  unsigned long long BIT10 = (BIT << 26) | (BIT << 5 );
  unsigned long long BIT11 = (BIT << 27) | (BIT << 4 );
  unsigned long long BIT12 = (BIT << 28) | (BIT << 3 );
  unsigned long long BIT13 = (BIT << 29) | (BIT << 2 );
  unsigned long long BIT14 = (BIT << 30) | (BIT << 1 );
  unsigned long long BIT15 = (BIT << 31) | BIT;

  const unsigned long long* logics;

  // Compress in cycles of 32 logicals
  for (int i = 0; i < nrOfLongs; ++i)
  {
    logics = &logicals[16 * i];

    compress[i] =
      (((logics[15] >> 15) | logics[15] << 15) & BIT0 ) |
      (((logics[14] >> 14) | logics[14] << 14) & BIT1 ) |
      (((logics[13] >> 13) | logics[13] << 13) & BIT2 ) |
      (((logics[12] >> 12) | logics[12] << 12) & BIT3 ) |
      (((logics[11] >> 11) | logics[11] << 11) & BIT4 ) |
      (((logics[10] >> 10) | logics[10] << 10) & BIT5 ) |
      (((logics[9 ] >> 9 ) | logics[9 ] << 9 ) & BIT6 ) |
      (((logics[8 ] >> 8 ) | logics[8 ] << 8 ) & BIT7 ) |
      (((logics[7 ] >> 7 ) | logics[7 ] << 7 ) & BIT8 ) |
      (((logics[6 ] >> 6 ) | logics[6 ] << 6 ) & BIT9 ) |
      (((logics[5 ] >> 5 ) | logics[5 ] << 5 ) & BIT10) |
      (((logics[4 ] >> 4 ) | logics[4 ] << 4 ) & BIT11) |
      (((logics[3 ] >> 3 ) | logics[3 ] << 3 ) & BIT12) |
      (((logics[2 ] >> 2 ) | logics[2 ] << 2 ) & BIT13) |
      (((logics[1 ] >> 1 ) | logics[1 ] << 1 ) & BIT14) |
      (  logics[0 ] & BIT15);
  }


  // Process remainder
  int remain = nrOfLogicals % 32;  // nr of logicals remaining
  if (remain == 0) return;

  unsigned long long remainLongs[16];  // at maximum nrOfRemainLongs equals 16
  int* remain_ints = reinterpret_cast<int*>(remainLongs);
                                       
  // Compress the remainder in identical manner as the blocks here (for random access) !!!!!!
  logics = &logicals[16 * nrOfLongs];

  const int nrOfRemainLongs = 1 + (remain - 1) / 2;  // per 2 logicals

  // please valgrind: only use initialized bytes for calculations
  memcpy(remainLongs, logics, remain * sizeof(int));
  memset(&remain_ints[remain], 0, (32 - remain) * 4);  // clear remaining ints

  unsigned long long compRes = 0;
  for (int remainNr = 0; remainNr < nrOfRemainLongs; ++remainNr)
  {
    compRes |= (remainLongs[remainNr] >> remainNr | remainLongs[remainNr] << remainNr) & (BIT << (31 - remainNr) | BIT << remainNr);
  }

  compress[nrOfLongs] = compRes;
}


// Still need code for endianness
// Compressor integers in the reange 0-127 and the NA-bit
void CompactIntToByte(char* outVec, const char* intVec, unsigned int nrOfInts)
{
  // Determine vector size in number of longs
  int nrOfLongs = (nrOfInts - 1) / 8;  // all but the last long

  // Calculate using long values
  unsigned long long* vecIn  = (unsigned long long*) intVec;
  unsigned long long* vecOut = (unsigned long long*) outVec;

  // Bit masks for 4 integers
  unsigned long long byte0 = (255LL << 32) | 255LL;
  unsigned long long byte1 = byte0 << 8;
  unsigned long long byte2 = byte0 << 16;
  unsigned long long byte3 = byte0 << 24;

  // Compact least significant byte

  int offset = -1;
  int blockIndex = 0;

  for (int i = 0; i != nrOfLongs; ++i)
  {
    vecOut[++offset] =
      (((vecIn[blockIndex + 3] >> 24) |  vecIn[blockIndex + 3]       ) & byte0) |
      (((vecIn[blockIndex + 2] >> 16) | (vecIn[blockIndex + 2] << 8 )) & byte1) |
      (((vecIn[blockIndex + 1] >> 8 ) | (vecIn[blockIndex + 1] << 16)) & byte2) |
      (((vecIn[blockIndex    ]      ) | (vecIn[blockIndex    ] << 24)) & byte3);

    blockIndex += 4;
  }

  // Process last integers

  int remain = nrOfInts - nrOfLongs * 8;

  unsigned long long intBuf[4] {0, 0, 0, 0};
  memcpy(intBuf, &vecIn[blockIndex], remain * 4);

  vecOut[++offset] =
    (((intBuf[3] >> 24) |  intBuf[3]       ) & byte0) |
    (((intBuf[2] >> 16) | (intBuf[2] << 8 )) & byte1) |
    (((intBuf[1] >> 8 ) | (intBuf[1] << 16)) & byte2) |
    (((intBuf[0]      ) | (intBuf[0] << 24)) & byte3);
}


void DecompactShortToInt(const char* compressedVec, char* intVec, unsigned int nrOfInts)
{
  // Determine vector size in number of longs
  int nrOfLongs = (nrOfInts - 1) / 4;  // all but the last long

  // Calculate using long values
  unsigned long long* vecCompress  = (unsigned long long*) compressedVec;
  unsigned long long* vecOut = (unsigned long long*) intVec;

  // Bit masks for 2 integers
  unsigned long long byte0  = (32767LL << 32) | 32767LL;
  unsigned long long byteNA = (1LL << 31);
  byteNA = byteNA | (byteNA << 32);

  // unsigned long long byte0 = (65535LL << 32) | 65535LL;

  // Compact least significant byte

  int blockIndex = -1;
  for (int i = 0; i != nrOfLongs; ++i)
  {
    unsigned long long val = vecCompress[i];
    vecOut[++blockIndex] = ((val >> 16) & byte0) | ( val        & byteNA);
    vecOut[++blockIndex] = ( val        & byte0) | ((val << 16) & byteNA);
  }

  // Process last integers
  int remain = nrOfInts - nrOfLongs * 4;

  unsigned long long intBuf[2] {0, 0};

  unsigned long long val = vecCompress[nrOfLongs];
  intBuf[0] = ((val >> 16) & byte0) | ( val        & byteNA);
  intBuf[1] = ( val        & byte0) | ((val << 16) & byteNA);

  // intBuf[0] = (val >> 16) & byte0;
  // intBuf[1] =  val        & byte0;

  memcpy(&vecOut[++blockIndex], intBuf, remain * 4);
}


// Still need code for endianess
void CompactIntToShort(char* outVec, const char* intVec, unsigned int nrOfInts)
{
  // Determine vector size in number of longs
  int nrOfLongs = (nrOfInts - 1) / 4;  // all but the last long

  // Calculate using long values
  unsigned long long* vecIn  = (unsigned long long*) intVec;
  unsigned long long* vecOut = (unsigned long long*) outVec;

  // Bit masks for 2 integers
  unsigned long long byte0 = (65535LL << 32) | 65535LL;
  unsigned long long byte1 = byte0 << 16;

  // Compact 4 integers per cycle

  int offset = -1;
  int blockIndex = 0;
  for (int i = 0; i != nrOfLongs; ++i)
  {
    vecOut[++offset] =
      (((vecIn[blockIndex + 1]  >> 16) | vecIn[blockIndex + 1]      ) & byte0) |
      (( vecIn[blockIndex    ]         | vecIn[blockIndex    ] << 16) & byte1);

    blockIndex += 2;
  }

  // Process last integers

  int remain = nrOfInts - nrOfLongs * 4;

  unsigned long long intBuf[2] {0, 0};
  memcpy(intBuf, &vecIn[blockIndex], remain * 4);

  vecOut[++offset] =
    (((intBuf[1]  >> 16) | intBuf[1]      ) & byte0) |
    (( intBuf[0]         | intBuf[0] << 16) & byte1);
}


void DecompactByteToInt(const char* compressedVec, char* intVec, unsigned int nrOfInts)
{
  // Determine vector size in number of longs
  int nrOfLongs = (nrOfInts - 1) / 8;  // all but the last long

  // Calculate using long values
  unsigned long long* vecCompress  = (unsigned long long*) compressedVec;
  unsigned long long* vecOut = (unsigned long long*) intVec;

  // Bit masks for 2 integers
  unsigned long long byte0  = (127LL << 32) | 127LL;
  unsigned long long byteNA = (1LL << 31);
  byteNA = byteNA | (byteNA << 32);

  // Compact least significant byte

  int blockIndex = -1;
  for (int i = 0; i != nrOfLongs; ++i)
  {
    unsigned long long val = vecCompress[i];
    vecOut[++blockIndex] = ((val >> 24) & byte0) | ( val       & byteNA);
    vecOut[++blockIndex] = ((val >> 16) & byte0) | ((val << 8) & byteNA);
    vecOut[++blockIndex] = ((val >> 8 ) & byte0) | ((val << 16) & byteNA);
    vecOut[++blockIndex] = ( val        & byte0) | ((val << 24) & byteNA);
  }

  // Process last integers
  int remain = nrOfInts - nrOfLongs * 8;

  unsigned long long intBuf[4] {0, 0, 0, 0};

  unsigned long long val = vecCompress[nrOfLongs];
    intBuf[0] = ((val >> 24) & byte0) | ( val       & byteNA);
    intBuf[1] = ((val >> 16) & byte0) | ((val << 8) & byteNA);
    intBuf[2] = ((val >> 8 ) & byte0) | ((val << 16) & byteNA);
    intBuf[3] = ( val        & byte0) | ((val << 24) & byteNA);

  memcpy(&vecOut[++blockIndex], intBuf, remain * 4);
}


// Function pointer to compression algorithm
typedef unsigned int (*CompAlgorithm)(char* dst, unsigned int dstCapacity, const char* src, unsigned int srcSize, int compressionLevel);


// Function pointer to decompression algorithm
typedef unsigned int (*DecompAlgorithm)(char* dst, unsigned int dstCapacity, const char* src, unsigned int  compressedSize);


// UNCOMPRESS,

unsigned int NoCompression(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  return 0;
}

unsigned int NoDecompression(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  return 0;
}


// LZ4,

unsigned int LZ4_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  return LZ4_compress_fast(src, dst, srcSize, dstCapacity, 101 - compressionLevel);  // no acceleration
}

unsigned int LZ4_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  return static_cast<unsigned int>(LZ4_decompress_fast(src, dst, dstCapacity)) != compressedSize;
}

unsigned int LZ4_INT_TO_BYTE_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int nrOfLongs = 1 + (srcSize - 1) / 32;  // srcSize is processed in blocks of 32 bytes

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_QUARTER];
  // char buf[nrOfLongs * 8];

  CompactIntToByte(buf, src, srcSize / 4);
  return LZ4_compress_fast(buf, dst, nrOfLongs * 8, dstCapacity, 101 - compressionLevel);  // no acceleration at compress == 100
}

unsigned int LZ4_INT_TO_BYTE_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int nrOfLongs = 1 + (dstCapacity - 1) / 32;  // srcSize is processed in blocks of 32 bytes
  int nrOfDstInts = dstCapacity / 4;

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_QUARTER];

  // Decompress
  unsigned int errorCode = static_cast<unsigned int>(LZ4_decompress_fast(src, (char*) buf, nrOfLongs * 8)) != compressedSize;
  DecompactByteToInt(buf, dst, nrOfDstInts);  // one integer per byte

  return errorCode;
}


unsigned int ZSTD_INT_TO_BYTE_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int nrOfLongs = 1 + (srcSize - 1) / 32;  // srcSize is processed in blocks of 32 bytes

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_QUARTER];

  CompactIntToByte(buf, src, srcSize / 4);

  return ZSTD_compress(dst, dstCapacity, (char*) buf,  nrOfLongs * 8,  (compressionLevel * ZSTD_maxCLevel()) / 100);
}

unsigned int ZSTD_INT_TO_BYTE_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  unsigned int nrOfLongs = 1 + (dstCapacity - 1) / 32;  // srcSize is processed in blocks of 32 bytes
  int nrOfDstInts = dstCapacity / 4;

  // Compress buffer
  // char buf[nrOfLongs * 8];
  char buf[MAX_SIZE_COMPRESS_BLOCK_QUARTER];

  // Decompress
  unsigned int errorCode = static_cast<unsigned int>(ZSTD_decompress((char*) buf, 8 * nrOfLongs, src, compressedSize) != 8 * nrOfLongs);
  DecompactByteToInt(buf, dst, nrOfDstInts);  // one integer per byte

  return errorCode;
}

unsigned int LZ4_INT_TO_SHORT_SHUF2_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int nrOfLongs = 1 + (srcSize - 1) / 16;  // srcSize is processed in blocks of 16 bytes

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_HALF];

  CompactIntToShort(buf, src, srcSize / 4);  // expecting a integer vector here
  return LZ4_compress_fast(buf, dst, nrOfLongs * 8, dstCapacity, 100 - compressionLevel);  // no acceleration at compress == 100
}

unsigned int LZ4_INT_TO_SHORT_SHUF2_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int nrOfLongs = 1 + (dstCapacity - 1) / 16;  // srcSize is processed in blocks of 32 bytes
  int nrOfDstInts = dstCapacity / 4;

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_HALF];

  // Decompress
  unsigned int errorCode = static_cast<unsigned int>(LZ4_decompress_fast(src, (char*) buf, nrOfLongs * 8)) != compressedSize;
  DecompactShortToInt(buf, dst, nrOfDstInts);  // one integer per byte

  return errorCode;
}

unsigned int ZSTD_INT_TO_SHORT_SHUF2_C(char* dst, unsigned int dstCapacity, const char* src, unsigned int srcSize, int compressionLevel)
{
  int nrOfLongs = 1 + (srcSize - 1) / 16;  // srcSize is processed in blocks of 16 bytes

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_HALF];

  CompactIntToShort(buf, src, srcSize / 4);  // expecting a integer vector here

  return ZSTD_compress(dst, dstCapacity, static_cast<char*>(buf), nrOfLongs * 8, (compressionLevel * ZSTD_maxCLevel()) / 100);
}

unsigned int ZSTD_INT_TO_SHORT_SHUF2_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  unsigned int nrOfLongs = 1 + (dstCapacity - 1) / 16;  // srcSize is processed in blocks of 32 bytes
  unsigned int nrOfDstInts = dstCapacity / 4;

  // Compress buffer
  char buf[MAX_SIZE_COMPRESS_BLOCK_HALF];

  // Decompress
  unsigned int errorCode = ZSTD_decompress(static_cast<char*>(buf), 8 * nrOfLongs, src, compressedSize) != 8 * nrOfLongs;

  DecompactShortToInt(buf, dst, nrOfDstInts);  // one integer per byte

  return errorCode;
}


// Factor algorithms

unsigned int INT_TO_BYTE_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  unsigned int nrOfLongs = 1 + (srcSize - 1) / 32;  // srcSize is processed in blocks of 32 bytes

  CompactIntToByte(dst, src, srcSize / 4);

  return 8 * nrOfLongs;
}

unsigned int INT_TO_BYTE_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  DecompactByteToInt(src, dst, dstCapacity / 4);  // one integer per byte

  return 0;
}

unsigned int INT_TO_SHORT_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int nrOfLongs = 1 + (srcSize - 1) / 16;  // srcSize is processed in blocks of 32 bytes

  CompactIntToShort(dst, src, srcSize / 4);

  return 8 * nrOfLongs;
}

unsigned int INT_TO_SHORT_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  DecompactShortToInt(src, dst, dstCapacity / 4);  // one integer per short

  return 0;
}

// LOGIC64

unsigned int LOGIC64_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int nrOfLogicals = srcSize / 4;  // srcSize must be a multiple of 4
  int nrOfLongs = 1 + (nrOfLogicals - 1) / 32;

  LogicCompr64(src, (unsigned long long*) dst, nrOfLogicals);
  return 8 * nrOfLongs;
}

unsigned int LOGIC64_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int nrOfLogicals = dstCapacity / 4;  // dstCapacity must be a multiple of 4

  LogicDecompr64(dst, (unsigned long long*) src, nrOfLogicals, 0);

  return 0;
}


// LZ4_LOGIC64

// Buffer src should contain an integer vector (with logicals)
// srcSize must be a multiple of 4
unsigned int LZ4_LOGIC64_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  // Create compression buffer
  int nrOfLogicals = srcSize / 4;
  int nrOfLongs = 1 + (nrOfLogicals - 1) / 32;

  // Compress buffer
  // unsigned long long buf[nrOfLongs];
  unsigned long long buf[MAX_SIZE_COMPRESS_BLOCK_128];

  LogicCompr64(src, buf, nrOfLogicals);
  return LZ4_compress_fast((char*) buf, dst, nrOfLongs * 8, dstCapacity, 100 - compressionLevel);  // no acceleration at compress == 100
}

unsigned int LZ4_LOGIC64_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int nrOfLogicals = dstCapacity / 4;
  int nrOfLongs = 1 + (nrOfLogicals - 1) / 32;

  // Compress buffer
  // unsigned long long buf[nrOfLongs];
  unsigned long long buf[MAX_SIZE_COMPRESS_BLOCK_128];

  // Decompress
  unsigned int errorCode = static_cast<unsigned int>(LZ4_decompress_fast(src, (char*) buf, 8 * nrOfLongs)) != compressedSize;
  LogicDecompr64(dst, (unsigned long long*) buf, nrOfLogicals, 0);

  return errorCode;
}


// ZSTD_LOGIC64

// Buffer src should contain an integer vector (with logicals)
// srcSize must be a multiple of 4
unsigned int ZSTD_LOGIC64_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  // Create compression buffer
  int nrOfLogicals = srcSize / 4;
  int nrOfLongs = 1 + (nrOfLogicals - 1) / 32;

  // Compress buffer
  // unsigned long long buf[nrOfLongs];
  unsigned long long buf[MAX_SIZE_COMPRESS_BLOCK_128];

  LogicCompr64(src, buf, nrOfLogicals);

  return ZSTD_compress(dst, dstCapacity, (char*) buf,  nrOfLongs * 8,  (compressionLevel * ZSTD_maxCLevel()) / 100);
}

unsigned int ZSTD_LOGIC64_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int nrOfLogicals = dstCapacity / 4;
  unsigned int nrOfLongs = 1 + (nrOfLogicals - 1) / 32;

    // Compress buffer
  // unsigned long long buf[nrOfLongs];
  unsigned long long buf[MAX_SIZE_COMPRESS_BLOCK_128];

  // Decompress
  unsigned int errorCode = static_cast<unsigned int>(ZSTD_decompress((char*) buf, 8 * nrOfLongs, src, compressedSize) != 8 * nrOfLongs);
  LogicDecompr64(dst, (unsigned long long*) buf, nrOfLogicals, 0);

  return errorCode;
}


// LZ4_SHUF4,

// Buffer src should contain an integer vector
// srcSize must be a multiple of 4
unsigned int LZ4_C_SHUF4(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int intSize = srcSize / 4;

  unsigned long long shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  ShuffleInt2((int*) src, (int*) shuffleBuf, intSize);
  return LZ4_compress_fast((char*) shuffleBuf, dst, srcSize, dstCapacity, 100 - compressionLevel);  // large acceleration
}

unsigned int LZ4_D_SHUF4(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int intSize = dstCapacity / 4;

  unsigned long long shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  unsigned int errorCode = static_cast<unsigned int>(LZ4_decompress_fast(src, (char*) shuffleBuf, dstCapacity)) != compressedSize;
  DeshuffleInt2((int*) shuffleBuf, (int*) dst, intSize);

  return errorCode;
}


// LZ4_SHUF8,

// Buffer src should contain an integer vector
// srcSize must be a multiple of 8
unsigned int LZ4_C_SHUF8(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int doubleSize = srcSize / 8;

  // double shuffleBuf[doubleSize];
  double shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  ShuffleReal((double*) src, shuffleBuf, doubleSize);
  return LZ4_compress_fast(reinterpret_cast<char*>(shuffleBuf), dst, srcSize, dstCapacity, 100 - compressionLevel);  // large acceleration
}

unsigned int LZ4_D_SHUF8(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int doubleSize = dstCapacity / 8;

  // double shuffleBuf[doubleSize];
  double shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  unsigned int errorCode = static_cast<unsigned int>(LZ4_decompress_fast(src, (char*) shuffleBuf, dstCapacity)) != compressedSize;
  DeshuffleReal(shuffleBuf, (double*) dst, doubleSize);

  return errorCode;
}


// ZSTD_SHUF8

unsigned int ZSTD_C_SHUF8(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int doubleSize = srcSize / 8;

  // double shuffleBuf[doubleSize];
  double shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  ShuffleReal((double*) src, shuffleBuf, doubleSize);
  return ZSTD_compress(dst, dstCapacity, (char*) shuffleBuf, srcSize, (compressionLevel * ZSTD_maxCLevel()) / 100);
}

unsigned int ZSTD_D_SHUF8(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int doubleSize = dstCapacity / 8;

  // double shuffleBuf[doubleSize];
  double shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  unsigned int errorCode = ZSTD_decompress((char*) shuffleBuf, dstCapacity, src, compressedSize) != dstCapacity;
  DeshuffleReal(shuffleBuf, (double*) dst, doubleSize);

  return errorCode;
}


// ZSTD,

unsigned int ZSTD_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  return ZSTD_compress(dst, dstCapacity, src,  srcSize,  (compressionLevel * ZSTD_maxCLevel()) / 100);
}

unsigned int ZSTD_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  return ZSTD_decompress(dst, dstCapacity, src, compressedSize) != dstCapacity;
}


// ZSTD_SHUF4,

unsigned int ZSTD_C_SHUF4(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel)
{
  int intSize = srcSize / 4;

  unsigned long long shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];
  // int shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_QUARTER];

  ShuffleInt2((int*) src, (int*) shuffleBuf, intSize);
  return ZSTD_compress(dst, dstCapacity, (char*) shuffleBuf, srcSize, (compressionLevel * ZSTD_maxCLevel()) / 100);
}

unsigned int ZSTD_D_SHUF4(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize)
{
  int intSize = dstCapacity / 4;

  unsigned long long shuffleBuf[MAX_SIZE_COMPRESS_BLOCK_8];

  unsigned int errorCode = ZSTD_decompress((char*) shuffleBuf, dstCapacity, src, compressedSize) != dstCapacity;
  DeshuffleInt2((int*) shuffleBuf, (int*) dst, intSize);

  return errorCode;
}


inline void smallmemcpy(char* dst, const char* src, int size)
{
  unsigned short longs = size / 2;

  unsigned long long * intP = (unsigned long long *) (dst);
  unsigned long long * intIn = (unsigned long long *) (src);

  for (int iPos = longs; iPos != 0; --iPos)
  {
    intP[iPos] = intIn[iPos];
  }

  int offset = 2 * longs;

  for (int mPos = offset; mPos < size; ++mPos)
  {
    dst[mPos] = src[mPos];
  }
}
