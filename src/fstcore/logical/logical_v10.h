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

#ifndef LOGICAL_v10_H
#define LOGICAL_v10_H


#include <istream>
#include <ostream>


// Logical vectors are always compressed to fill all available bits (factor 16 compression).
// On top of that, we can compress the resulting bytes with a custom compressor.
void fdsWriteLogicalVec_v10(std::ofstream &myfile, int* boolVector, unsigned long long nrOfLogicals, int compression,
  std::string annotation, bool hasAnnotation);


void fdsReadLogicalVec_v10(std::istream &myfile, int* boolVector, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size);

#endif // LOGICAL_v10_H
