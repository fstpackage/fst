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

#ifndef BYTE_BLOCK_V13_H
#define BYTE_BLOCK_V13_H

#include <fstream>

#include <interface/ibyteblockcolumn.h>

void fdsWriteByteBlockVec_v13(std::ofstream& fst_file, IByteBlockColumn* byte_block_writer,
  uint64_t nr_of_rows, uint32_t compression);

void read_byte_block_vec_v13(std::istream& fst_file, IByteBlockColumn* byte_block, uint64_t block_pos, uint64_t start_row,
  uint64_t length, uint64_t size);

#endif // BYTE_BLOCK_V13_H
