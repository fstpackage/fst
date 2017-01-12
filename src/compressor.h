/*
  fst - An R-package for ultra fast storage and retrieval of datasets.
  Header File
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

#ifndef COMPRESSOR_H
#define COMPRESSOR_H

#include <Rcpp.h>

#include <compression.h>

#define NR_OF_ALGORITHMS 15
#define MAX_TARGET_REP_SIZE 8
#define MAX_SOURCE_REP_SIZE 128
#define MAX_TARGET_BUFFER 8192  // 16384  / 2 

// Compression algorithm types. Used for determining the maximum compression buffer size.
enum CompAlgoType
{
  UNCOMPRESSED,
  LZ4_TYPE,
  ZSTD_TYPE,
  LZ4_LOGIC64_TYPE,
  LOGIC64_TYPE,
  ZSTD_LOGIC64_TYPE,
  LZ4_INT_TO_BYTE_TYPE,
  LZ4_INT_TO_SHORT_TYPE,
  INT_TO_BYTE_TYPE,
  INT_TO_SHORT_TYPE,
  ZSTD_INT_TO_BYTE_TYPE
};


enum CompAlgo
{
  UNCOMPRESS,
  LZ4,
  LZ4_SHUF4,
  ZSTD,
  ZSTD_SHUF4,
  LZ4_SHUF8,
  ZSTD_SHUF8,
  LZ4_LOGIC64,
  LOGIC64,
  ZSTD_LOGIC64,
  LZ4_INT_TO_BYTE,
  LZ4_INT_TO_SHORT_SHUF2,
  INT_TO_BYTE,
  INT_TO_SHORT,
  ZSTD_INT_TO_BYTE
};


// Source data minimum repeat length
extern unsigned int fixedRatioSourceRepSize[NR_OF_ALGORITHMS];

// Target data minimum repeat length
extern unsigned int fixedRatioTargetRepSize[NR_OF_ALGORITHMS];


class Decompressor
{
public:

  /**
   Constructor for a compressor with two competing compression algorithms.

   @param algo1 First decompression algorithm.
   @param algo2 Second decompression algorithm.
   @param algo3 Thirth decompression algorithm.
   @param algo4 Fourth decompression algorithm.
   */
  Decompressor() {};

  ~Decompressor() { };

  int Decompress(unsigned int algo, char* dst, unsigned int dstCapacity, const char* src, unsigned int compressedSize);
};


// Base class for compressors.
class Compressor
{
public:

   // pure virtual function providing interface for a compressor.
  virtual int Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm) = 0;
  virtual int CompressBufferSize(int maxBlockSize) = 0;

  virtual ~Compressor() {}
};


// A compressor that always has an identical compression ratio
class FixedRatioCompressor : public Compressor
{
private:
  CompAlgorithm a1;
  CompAlgo algo;
  int repSize, targetRepSize;

public:

  /**
   Constructor for a single algorithm compressor.

   @param algo1 First compression algorithm.
   @param algo2 Second compression algorithm.
   */
  FixedRatioCompressor(CompAlgo algo);

  int CompressBufferSize(int maxBlockSize);

  /**
  Getter for the minimum source blockSize that defines repetition in the compression algorithm

  @return Minimum source repetition
  */
  int SourceRepetitionSize() { return repSize; }

  /**
  Getter for the target output size if a block of SourceRepetitionSize bytes is compressed

  @return Minimum target repetition
  */
  int TargetRepetitionSize() { return targetRepSize; }

  /**
  Compress src into dst

  @param dst Destination buffer
  @param dstCapacity Size of destination buffer
  @param src Source buffer
  @param srcSize Size of source buffer
  @return Resulting number of bytes in the compressed data
  */
  int Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm);
};


class SingleCompressor : public Compressor
{
private:
  CompAlgorithm a1;
  CompAlgo algo1;
  int compLevel;

public:

  /**
   Constructor for a single algorithm compressor.

   @param algo1 First compression algorithm.
   @param algo2 Second compression algorithm.
   */
  SingleCompressor(CompAlgo algo1, int compressionLevel);

  int CompressBufferSize(int maxBlockSize);

  /**
  Compress src into dst using compressionLevel (0 - 100)

  @param dst Destination buffer
  @param dstCapacity Size of destination buffer
  @param src Source buffer
  @param srcSize Size of source buffer
  @return Resulting number of bytes in the compressed data
  */
  int Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm);
};


class DualCompressor : public Compressor
{
private:
  CompAlgorithm a1, a2;
  CompAlgo algo1, algo2;
  int compLevel1, compLevel2;

  int lastCount;
  float a1Count;
  int a1Ratio;
  int lastSize1;
  int lastSize2;

public:

  /**
   Constructor for a compressor with two competing compression algorithms.

   @param algo1 First compression algorithm.
   @param algo2 Second compression algorithm.
   */
  DualCompressor(CompAlgo algo1, CompAlgo algo2, int compressionLevel1, int compressionLevel2);

  int CompressBufferSize(int maxBlockSize);

  /**
  Compress src into dst using compressionLevel (0 - 100)

  @param dst Destination buffer
  @param dstCapacity Size of destination buffer
  @param src Source buffer
  @param srcSize Size of source buffer
  @return Resulting number of bytes in the compressed data
  */
  int Compress(char* dst, unsigned int dstCapacity, const char* src,  unsigned int srcSize, CompAlgo &compAlgorithm);
};



class StreamCompressor
{
public:
  virtual int Compress(std::ofstream &myfile, const char* src, unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm) = 0;

  virtual int CompressBufferSize(unsigned int srcSize) = 0;
  virtual int CompressBufferSize() = 0;

  virtual ~StreamCompressor() {};
};



/**
 A null-compressor that just writes to file without compression.
*/
class StreamFixedCompressor : StreamCompressor
{
public:

  int Compress(std::ofstream &myfile, const char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm);
};


/**
 A compressor that works by producing uncompressed and compressed blocks in a specific ratio.
 The chosen ratio determines the overall compression ratio. A ratio of zero means that only
 uncompressed blocks are used. A maximum value of 100 compresses all blocks.
*/
class StreamLinearCompressor : public StreamCompressor
{
private:

  Compressor* compress;
  unsigned int compBlockCount;
  unsigned int nextCompBlock;
  unsigned int curBlock;
  float compFactor;
  int compBufSize;

public:

/**
   Constructor for a StreamLinearCompressor

   @param compressor Compressor that is to be used for the compressed blocks. No specific parameters can be set for
          the compressor.
   @param compressionLevel Value 0 - 100 indicating the compression level.
   @param maxBlockSize Maximum size of the input buffer for compression.
*/
  StreamLinearCompressor(Compressor *compressor, float compressionLevel);

  int CompressBufferSize();

  int CompressBufferSize(unsigned int srcSize);

  /**
    Compress src to myfile. Buffer compBuf can be used as a buffer if required.
    Make sure it is at least CompressBufferSize() long.

    @param myfile ofstream to which you want to write the compressed data.
    @param src Source data.
    @param srcSize Size of the source data (in bytes).
    @param compBuf Buffer to store temporary data.
    @param compAlgorithm Algorithm that was used for compressing the data.
  */

  int Compress(std::ofstream &myfile, const char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm);
};



/**
 A compressor that works by producing compressed blocks with a single algorithm.
*/
class StreamSingleCompressor : public StreamCompressor
{
private:

  Compressor* compress;
  int compBufSize;

public:

/**
   Constructor for a StreamLinearCompressor

   @param compressor Compressor that is to be used for the compressed blocks. No specific parameters can be set for
          the compressor.
   @param compressionLevel Value 0 - 100 indicating the compression level.
   @param maxBlockSize Maximum size of the input buffer for compression.
*/
  StreamSingleCompressor(Compressor *compressor);

  int CompressBufferSize();

  int CompressBufferSize(unsigned int srcSize);

  /**
    Compress src to myfile. Buffer compBuf can be used as a buffer if required.
    Make sure it is at least CompressBufferSize() long.

    @param myfile ofstream to which you want to write the compressed data.
    @param src Source data.
    @param srcSize Size of the source data (in bytes).
    @param compBuf Buffer to store temporary data.
    @param compAlgorithm Algorithm that was used for compression.
  */
  int Compress(std::ofstream &myfile, const char* src, unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm);
};


/**
 A compressor that works by producing compressed blocks with two algorithms in a specific ratio.
 The second algorithm is expected to be a slower but stronger algorithm.
 The chosen ratio determines the overall compression ratio and speed. A ratio of zero means that only
 the first algorithm is used for compression. A maximum value of 100 compresses all blocks with the
 second algorithm.
*/
class StreamCompositeCompressor : public StreamCompressor
{
private:

  Compressor* compress1;
  Compressor* compress2;
  unsigned int compBlockCount;
  unsigned int nextCompBlock;
  unsigned int curBlock;
  float compFactor;
  int compBufSize;

public:

/**
   Constructor for a StreamLinearCompressor

   @param compressor Compressor that is to be used for the compressed blocks. No specific parameters can be set for
          the compressor.
   @param compressionLevel Value 0 - 100 indicating the compression level.
   @param maxBlockSize Maximum size of the input buffer for compression.
*/
  StreamCompositeCompressor(Compressor *compressor1, Compressor *compressor2, float compressionLevel);

  int CompressBufferSize();

  int CompressBufferSize(unsigned int srcSize);

  /**
    Compress src to myfile. Buffer compBuf can be used as a buffer if required.
    Make sure it is at least CompressBufferSize() long.

    @param myfile ofstream to which you want to write the compressed data.
    @param src Source data.
    @param srcSize Size of the source data (in bytes).
    @param compBuf Buffer to store temporary data.
    @param compAlgorithm Algorithm that was used for compression.
  */
  int Compress(std::ofstream &myfile, const char* src,  unsigned int srcSize, char* compBuf, CompAlgo &compAlgorithm);
};


#endif  // COMPRESSOR_H
