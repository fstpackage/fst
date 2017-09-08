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


#ifndef IFST_COLUMN_H
#define IFST_COLUMN_H

#include <interface/istringwriter.h>


// Column types available in fst
// Only add to the end to support previous format versions
enum FstColumnType
{
	UNKNOWN = 1,
	CHARACTER,
	FACTOR,
	INT_32,
	DOUBLE_64,
	BOOL_32,
	INT_64,
	BYTE
};


// Column attributes available in fst
// Only add to the end to support previous format versions
enum FstColumnAttribute
{
	NONE = 1,                // unknown type
	CHARACTER_BASE,          // default character type
	FACTOR_BASE,             // default factor type (character vector levels with integer vector)
	INT_32_BASE,             // default integer type
	INT_32_TIME_SECONDS,     // number of seconds since epoch
	INT_32_DATE_DAYS,        // number of days since epoch
	DOUBLE_64_BASE,          // default double type
	DOUBLE_64_DATE_DAYS,     // number of days since epoch
	DOUBLE_64_TIME_SECONDS,  // number of fractional seconds since epoch
	BOOL_32_BASE,            // 3 value boolean: 0 (false), 1 (true) and INT_MIN (NA)
	INT_64_BASE,             // default int64 type
	INT_64_TIME_NANO,        // number of nanoseconds since epoch
	INT_64_TIME_MICRO,       // number of microseconds since epoch
	BYTE_BASE                // default byte type
};


// The abstract column and array interfaces function as a bridge between the actual data and fst


class IStringArray
{
public:

  virtual ~IStringArray() {};

  virtual void AllocateArray(int vecLength) = 0;

  virtual void SetElement(int elementNr, const char* str) = 0;

  virtual void SetElement(int elementNr, const char* str, int strLen) = 0;

  virtual const char* GetElement(int elementNr) = 0;

  virtual int Length() = 0;
};


class IStringColumn
{
public:

  virtual ~IStringColumn() {}

  virtual void AllocateVec(unsigned int vecLength) = 0;

  virtual void SetEncoding(StringEncoding stringEncoding) = 0;

  virtual void BufferToVec(unsigned int nrOfElements, unsigned int startElem, unsigned int endElem,
    unsigned int vecOffset, unsigned int* sizeMeta, char* buf) = 0;

  virtual const char* GetElement(int elementNr) = 0;
};


class IFactorColumn
{
public:
  virtual ~IFactorColumn() {};
  virtual int* LevelData() = 0;
  virtual IStringColumn* Levels() = 0;
};


class IInt64Column
{
public:
  virtual ~IInt64Column() {};
  virtual long long* Data() = 0;
};


class IIntegerColumn
{
public:
  virtual ~IIntegerColumn() {};
  virtual int* Data() = 0;
};


class IByteColumn
{
public:
	virtual ~IByteColumn() {};
	virtual char* Data() = 0;
};


class IDoubleColumn
{
public:
  virtual ~IDoubleColumn() {};
  virtual double* Data() = 0;
};


class ILogicalColumn
{
public:
  virtual ~ILogicalColumn() {};
  virtual int* Data() = 0;
};


#endif // IFST_COLUMN_H

