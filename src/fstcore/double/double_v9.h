/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  fstlib is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  fstlib is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with fstlib.  If not, see <http://www.gnu.org/licenses/>.

  You can contact the author at :
  - fstlib source repository : https://github.com/fstPackage/fstlib
*/

#ifndef DOUBLE_v9_H
#define DOUBLE_v9_H

// System libraries
#include <ostream>
#include <istream>


void fdsWriteRealVec_v9(std::ofstream &myfile, double* doubleVector, unsigned long long nrOfRows, unsigned int compression,
  std::string annotation, bool hasAnnotation);

void fdsReadRealVec_v9(std::istream &myfile, double* doubleVector, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size, std::string &annotation, bool &hasAnnotation);

#endif // DOUBLE_v9_H
