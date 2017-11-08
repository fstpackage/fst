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


#ifndef ISTRINGWRITER_H
#define ISTRINGWRITER_H

enum StringEncoding
{
	NATIVE = 0,
	LATIN1,
	UTF8
};


class IStringWriter
{
public:
  unsigned int* strSizes;
  unsigned int* naInts;
  unsigned int bufSize;
  char* activeBuf;
  unsigned long long vecLength;

  virtual ~IStringWriter() {}

  virtual StringEncoding Encoding() = 0;

  virtual void SetBuffersFromVec(unsigned long long startCount, unsigned long long endCount) = 0;
};


#endif  // ISTRINGWRITER_H

