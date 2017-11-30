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

#ifndef COMPRESSION_H
#define COMPRESSION_H


size_t MAX_compressBound(size_t srcSize);

// of LZ4 and ZSTD
// #define LZ4_COMPRESSBOUND(isize)  ((unsigned)(isize) > (unsigned)LZ4_MAX_INPUT_SIZE ? 0 : (isize) + ((isize)/255) + 16)

// The size of outVec is expected to be 2 times nrOfDoubles
void ShuffleReal(double* inVec, double* outVec, int nrOfDoubles);


void DeshuffleReal(double* inVec, double* outVec, int nrOfDoubles);


// The size of outVec must be equal to nrOfInts
void ShuffleInt2(int* inVec, int* outVec, int nrOfInts);


void DeshuffleInt2(int* inVec, int* outVec, int nrOfInts);


// The first nrOfDiscard decompressed logicals are discarded. Parameter nrOfLogicals includes these discarded values,
// so nrOfLogicals must be equal or larger than nrOfDiscard.
void LogicDecompr64(char* logicalVec, const unsigned long long* compBuf, int nrOfLogicals, int nrOfDiscard);


// Compression buffer should be at least 1 + (nrOfLogicals - 1) / 256 elements in length (factor 32)
void LogicCompr64(const char* logicalVec, unsigned long long* compress, int nrOfLogicals);


// Still need code for endianness
// Compressor integers in the reange 0-127 and the NA-bit
void CompactIntToByte(char* outVec, const char* intVec, unsigned int nrOfInts);


void DecompactShortToInt(const char* compressedVec, char* intVec, unsigned int nrOfInts);


// Still need code for endianess
void CompactIntToShort(char* outVec, const char* intVec, unsigned int nrOfInts);


void DecompactByteToInt(const char* compressedVec, char* intVec, unsigned int nrOfInts);


// Function pointer to compression algorithm
typedef unsigned int (*CompAlgorithm)(char* dst, unsigned int dstCapacity, const char* src, unsigned int srcSize, int compressionLevel);


// Function pointer to decompression algorithm
typedef unsigned int (*DecompAlgorithm)(char* dst, unsigned int dstCapacity, const char* src, unsigned int  compressedSize);


// UNCOMPRESS,

unsigned int NoCompression(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int NoDecompression(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// LZ4,

unsigned int LZ4_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LZ4_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


unsigned int LZ4_INT_TO_BYTE_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LZ4_INT_TO_BYTE_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


unsigned int ZSTD_INT_TO_BYTE_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int ZSTD_INT_TO_BYTE_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


unsigned int LZ4_INT_TO_SHORT_SHUF2_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LZ4_INT_TO_SHORT_SHUF2_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


unsigned int ZSTD_INT_TO_SHORT_SHUF2_C(char* dst, unsigned int dstCapacity, const char* src, unsigned int srcSize, int compressionLevel);


unsigned int ZSTD_INT_TO_SHORT_SHUF2_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// Factor algorithms

unsigned int INT_TO_BYTE_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int INT_TO_BYTE_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


unsigned int INT_TO_SHORT_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int INT_TO_SHORT_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// LOGIC64

unsigned int LOGIC64_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LOGIC64_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// LZ4_LOGIC64

// Buffer src should contain an integer vector (with logicals)
// srcSize must be a multiple of 4
unsigned int LZ4_LOGIC64_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LZ4_LOGIC64_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// ZSTD_LOGIC64

// Buffer src should contain an integer vector (with logicals)
// srcSize must be a multiple of 4
unsigned int ZSTD_LOGIC64_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int ZSTD_LOGIC64_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// LZ4_SHUF4,

// Buffer src should contain an integer vector
// srcSize must be a multiple of 4
unsigned int LZ4_C_SHUF4(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LZ4_D_SHUF4(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// LZ4_SHUF8,

// Buffer src should contain an integer vector
// srcSize must be a multiple of 8
unsigned int LZ4_C_SHUF8(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int LZ4_D_SHUF8(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// ZSTD_SHUF8

unsigned int ZSTD_C_SHUF8(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int ZSTD_D_SHUF8(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// ZSTD,

unsigned int ZSTD_C(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int ZSTD_D(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


// ZSTD_SHUF4,

unsigned int ZSTD_C_SHUF4(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, int compressionLevel);


unsigned int ZSTD_D_SHUF4(char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);


#endif  // COMPRESSION_H
