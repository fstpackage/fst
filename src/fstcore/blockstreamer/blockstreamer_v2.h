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

#ifndef BLOCKSTORE_H
#define BLOCKSTORE_H

#include <fstream>

#include <compression/compressor.h>

// Method for writing column data of any type to a ofstream.
void fdsStreamUncompressed_v2(std::ofstream &myfile, char* vec, unsigned long long vecLength, int elementSize, int blockSizeElems,
  FixedRatioCompressor* fixedRatioCompressor, std::string annotation, bool hasAnnotation);


// Method for writing column data of any type to a stream.
void fdsStreamcompressed_v2(std::ofstream &myfile, char* colVec, unsigned long long nrOfRows, int elementSize,
  StreamCompressor* streamCompressor, int blockSizeElems, std::string annotation, bool hasAnnotation);


void fdsReadColumn_v2(std::istream &myfile, char* outVec, unsigned long long blockPos, unsigned long long startRow, unsigned long long length,
  unsigned long long size, int elementSize, std::string &annotation, int maxbatchSize, bool &hasAnnotation);


#endif // BLOCKSTORE_H
