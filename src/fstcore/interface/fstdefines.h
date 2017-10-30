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


#ifndef FSTDEFINES_H
#define FSTDEFINES_H


// Format related defines
#define FST_VERSION          1                  // version of fst codebase
#define TABLE_META_SIZE      44                 // size of table meta-data block
#define FST_FILE_ID          0xa91c12f8b245a71d // identifies a fst file or memory block
#define FST_HASH_SEED        912824571          // default seed used for xxhash algorithm
#define CHUNKSET_HEADER_SIZE 76                 // size of chunkset header
#define CHUNK_INDEX_SIZE     96                 // size of chunk index header
#define DATA_INDEX_SIZE      24                 // size of data index header
#define CHAR_HEADER_SIZE     8                  // meta data header size
#define CHAR_INDEX_SIZE      16                 // size of 1 index entry
#define BASIC_HEAP_SIZE      1048576            // starting size of heap buffer

// Format flags
#define FLAG_INDIRECT_HEADER 1                  // Next value is the absolute position of the extended header

// Read batch sizes per type
#define BATCH_SIZE_READ_INT             100
#define BATCH_SIZE_READ_LOGICAL         400
#define BATCH_SIZE_READ_INT64           100
#define BATCH_SIZE_READ_FACTOR          100
#define BATCH_SIZE_READ_DOUBLE          100
#define BATCH_SIZE_READ_BYTE            100

// Cache-size related defines
#define CACHEFACTOR						1
#define PREV_NR_OF_BLOCKS               48                          // default number of blocks for in-memory compression
#define BLOCKSIZE						            16384 * CACHEFACTOR			    // number of bytes in default compression block
#define HASH_SIZE						            4096            			      // number of bytes in default compression block
#define MAX_CHAR_STACK_SIZE				      32768						            // number of characters in default compression block
#define BLOCKSIZE_CHAR					        2047						            // number of characters in default compression block
#define PREF_BLOCK_SIZE					        16384 * CACHEFACTOR			    // BlockStreamer
#define MAX_SIZE_COMPRESS_BLOCK			    16384 * CACHEFACTOR			    // Compression
#define MAX_SIZE_COMPRESS_BLOCK_HALF	  8192 * CACHEFACTOR			    // Compression
#define MAX_SIZE_COMPRESS_BLOCK_QUARTER 4096 * CACHEFACTOR			    // Compression
#define MAX_SIZE_COMPRESS_BLOCK_8		    2048 * CACHEFACTOR			    // Compression
#define MAX_SIZE_COMPRESS_BLOCK_128		  128 * CACHEFACTOR			      // Compression
#define MAX_COMPRESSBOUND				        17040						            // maximum compression buffer at source size of MAX_SIZE_COMPRESS_BLOCK
#define MAX_TARGET_BUFFER				        BLOCKSIZE * CACHEFACTOR / 2 // 16384  / 2  (Compressor)
#define BLOCKSIZE_REAL					        2048 * CACHEFACTOR			    // number of doubles in default compression block
#define BLOCKSIZE_INT64					        2048 * CACHEFACTOR			    // number of long long in default compression block
#define BLOCKSIZE_INT					          4096 * CACHEFACTOR			    // number of integers in default compression block
#define BLOCKSIZE_BYTE					        16384 * CACHEFACTOR			    // number of bytes in default compression block

// fst specific errors
#define FSTERROR_NOT_IMPLEMENTED     "Feature not implemented yet"
#define FSTERROR_ERROR_OPENING_FILE  "Error opening fst file for reading, please check access rights and file availability"
#define FSTERROR_NO_APPEND           "This version of the fst file format does not allow appending data"
#define FSTERROR_DAMAGED_HEADER      "It seems the file header was damaged or incomplete"
#define FSTERROR_DAMAGED_CHUNKINDEX  "The chunk index header is damaged or incomplete"
#define FSTERROR_DAMAGED_METADATA    "The file contains damaged or missing metadata"
#define FSTERROR_INCORRECT_COL_COUNT "Data frame has an incorrect amount of columns"
#define FSTERROR_NON_FST_FILE        "File format was not recognised as a fst file"
#define FSTERROR_NO_DATA             "The dataset contains no data"
#define FSTERROR_ERROR_OPEN_WRITE    "There was an error creating the file, please check path"
#define FSTERROR_ERROR_OPEN_READ     "There was an error opening the file, it seems to be incomplete or damaged."
#define FSTERROR_UPDATE_FST          "Incompatible fst file: file was created by a newer version of fst"

#define FST_NA_INT					         0x80000000

#endif // FSTDEFINES_H
