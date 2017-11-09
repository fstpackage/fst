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

#ifndef CHARACTER_V6_H
#define CHARACTER_V6_H


#include <iostream>
#include <fstream>

#include "interface/istringwriter.h"
#include "interface/ifstcolumn.h"


void fdsWriteCharVec_v6(std::ofstream &myfile, IStringWriter* blockRunner, int compression, StringEncoding stringEncoding);


void fdsReadCharVec_v6(std::istream &myfile, IStringColumn* blockReader, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long vecLength, unsigned long long size);


#endif  // CHARACTER_V6_H

