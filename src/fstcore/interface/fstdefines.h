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


#ifndef FSTDEFINES_H
#define FSTDEFINES_H


// Format related defines
#define FST_VERSION         1                  // version of fst codebase
#define TABLE_META_SIZE     24                 // size of table meta-data block
#define FST_FILE_ID         0xa91c12f8b245a71d // identifies a fst file or memory block
#define FST_HASH_SEED       912824571          // seed used by in-memory xxhash
#define CHUNK_INDEX_SIZE    144                // size of fixed component of vertical chunk index
#define CHAR_HEADER_SIZE    8                  // meta data header size
#define CHAR_INDEX_SIZE     16                 // size of 1 index entry
#define BASIC_HEAP_SIZE     1048576            // starting size of heap buffer

// Cache-size related defines
#define CACHEFACTOR						1
#define PREV_NR_OF_BLOCKS               48                          // default number of blocks for in-memory compression
#define BLOCKSIZE						16384 * CACHEFACTOR			// number of bytes in default compression block
#define HASH_SIZE						4096            			// number of bytes in default compression block
#define MAX_CHAR_STACK_SIZE				32768						// number of characters in default compression block
#define BLOCKSIZE_CHAR					2047						// number of characters in default compression block
#define PREF_BLOCK_SIZE					16384 * CACHEFACTOR			// BlockStreamer
#define MAX_SIZE_COMPRESS_BLOCK			16384 * CACHEFACTOR			// Compression
#define MAX_SIZE_COMPRESS_BLOCK_HALF	8192 * CACHEFACTOR			// Compression
#define MAX_SIZE_COMPRESS_BLOCK_QUARTER 4096 * CACHEFACTOR			// Compression
#define MAX_SIZE_COMPRESS_BLOCK_8		2048 * CACHEFACTOR			// Compression
#define MAX_SIZE_COMPRESS_BLOCK_128		128 * CACHEFACTOR			// Compression
#define MAX_COMPRESSBOUND				17036						// maximum compression buffer at source size of MAX_SIZE_COMPRESS_BLOCK
#define MAX_TARGET_BUFFER				BLOCKSIZE * CACHEFACTOR / 2 // 16384  / 2  (Compressor)
#define BLOCKSIZE_REAL					2048 * CACHEFACTOR			// number of doubles in default compression block
#define BLOCKSIZE_INT64					2048 * CACHEFACTOR			// number of long long in default compression block
#define BLOCKSIZE_INT					4096 * CACHEFACTOR			// number of integers in default compression block
#define BLOCKSIZE_BYTE					16384 * CACHEFACTOR			// number of bytes in default compression block

// fst specific errors
#define FSTERROR_NOT_IMPLEMENTED     "Feature not implemented yet"
#define FSTERROR_ERROR_OPENING_FILE  "Error opening fst stream"
#define FSTERROR_NO_APPEND           "This version of the fst file format does not allow appending data"
#define FSTERROR_DAMAGED_HEADER      "Error reading file header, your fst file is incomplete or damaged"
#define FSTERROR_INCORRECT_COL_COUNT "Data frame has an incorrect amount of columns"
#define FSTERROR_NON_FST_FILE        "File format was not recognised as a fst file"

#define FST_NA_INT					 0x80000000

#endif // FSTDEFINES_H
