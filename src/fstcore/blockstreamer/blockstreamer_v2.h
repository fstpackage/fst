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

#ifndef BLOCKSTORE_H
#define BLOCKSTORE_H

#include <fstream>

#include <compression/compressor.h>

// Method for writing column data of any type to a ofstream.
void fdsStreamUncompressed_v2(std::ofstream& myfile, char* vec, unsigned long long vecLength, int elementSize, int blockSizeElems,
                              FixedRatioCompressor* fixedRatioCompressor, std::string annotation, bool hasAnnotation);


// Method for writing column data of any type to a stream.
void fdsStreamcompressed_v2(std::ofstream& myfile, char* colVec, unsigned long long nrOfRows, int elementSize,
                            StreamCompressor* streamCompressor, int blockSizeElems, std::string annotation, bool hasAnnotation);


void fdsReadColumn_v2(std::istream& myfile, char* outVec, unsigned long long blockPos, unsigned long long startRow, unsigned long long length,
                      unsigned long long size, int elementSize, std::string& annotation, int maxbatchSize, bool& hasAnnotation);


#endif // BLOCKSTORE_H
