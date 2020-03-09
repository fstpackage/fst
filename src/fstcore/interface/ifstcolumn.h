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


#ifndef IFST_COLUMN_H
#define IFST_COLUMN_H

#include <string>

#include <interface/istringwriter.h>


// Column scaling in power of 10
// Applicable to INT_32, DOUBLE_64 and INT_64 type.
// Only add to the end to support previous format versions
#define SCALE_PICO    (-12)
#define SCALE_NANO     (-9)
#define SCALE_MICRO    (-6)
#define SCALE_MILLI    (-3)
#define SCALE_UNITY       0
#define SCALE_KILO        3
#define SCALE_MEGA        6
#define SCALE_GIGA        9
#define SCALE_TERA       12


// Column types available in fst
// Only add to the end to support previous format versions
// This type translates to a type number in the fst format:
//
// CHARACTER  | 6
// FACTOR,    | 7
// INT_32,    | 8
// DOUBLE_64  | 9
// BOOL_2     | 10
// INT_64     | 11
// BYTE       | 12
// BYTE_BLOCK | 13
//
enum FstColumnType
{
	UNKNOWN = 1,
	CHARACTER,    // character vector
	FACTOR,       // factor with character vector levels (0 = NA)
	INT_32,       // 32-bit signed integer vector
	DOUBLE_64,    // 64-bit double vector
	BOOL_2,       // 2-bit boolean value (00 = false, 01 = true and 10 = NA)
	INT_64,       // 64-bit signed integer vector
	BYTE,         // byte vector
    BYTE_BLOCK    // vector of custom sized byte blocks
};


// Column attributes available in fst
// Only add to the end to support previous format versions
enum FstColumnAttribute
{
  NONE = 1,                       // unknown type
  CHARACTER_BASE,                 // default character type
  FACTOR_BASE,                    // default factor type (character vector levels with integer vector)
  FACTOR_ORDERED,                 // factor with ordered levels
  INT_32_BASE,                    // default integer type
  INT_32_TIMESTAMP_SECONDS,       // number of seconds since epoch. Annotation holds the timezone.
  INT_32_TIMEINTERVAL_SECONDS,    // number of fractional seconds between two moments in time.
  INT_32_DATE_DAYS,               // number of days since epoch
  INT_32_TIME_OF_DAY_SECONDS,     // number of seconds since the start of day [scale is FstTimeScale enum]
  DOUBLE_64_BASE,                 // default double type
  DOUBLE_64_DATE_DAYS,            // number of days since epoch
  DOUBLE_64_TIMESTAMP_SECONDS,    // number of fractional seconds since epoch. Annotation holds the timezone.
  DOUBLE_64_TIMEINTERVAL_SECONDS, // number of fractional seconds between two moments in time [scale is FstTimeScale enum]
  DOUBLE_64_TIME_OF_DAY_SECONDS,  // number of seconds since the start of day [scale is FstTimeScale enum]
  BOOL_2_BASE,                    // 3 value boolean: 0 (false), 1 (true) and INT_MIN (NA)
  INT_64_BASE,                    // default int64 type
  INT_64_TIME_SECONDS,            // number of seconds since epoch [scale is FstTimeScale enum]
  BYTE_BASE                       // default byte type
};


// Default scale
// Only add to the end to support previous format versions
enum FstScale
{
  UNIT = 0
};


// Available time scales
// Only add to the end to support previous format versions
enum FstTimeScale
{
  NANOSECONDS = 1,
  MICROSECONDS,
  MILLISECONDS,
  SECONDS,
  MINUTES,
  HOURS,
  DAYS,
  YEARS
};


// The abstract column and array interfaces function as a bridge between the actual data and fst

class IStringArray
{
public:

  virtual ~IStringArray() {};

  virtual void AllocateArray(uint64_t vecLength) = 0;

  virtual void SetEncoding(StringEncoding string_encoding) = 0;

  virtual void SetElement(uint64_t elementNr, const char* str) = 0;

  virtual const char* GetElement(uint64_t elementNr) = 0;

  virtual uint64_t Length() = 0;
};


class IStringColumn
{
public:

  virtual ~IStringColumn() {}

  virtual void AllocateVec(uint64_t vecLength) = 0;

  virtual void SetEncoding(StringEncoding stringEncoding) = 0;

  virtual StringEncoding GetEncoding() = 0;

  virtual void BufferToVec(uint64_t nrOfElements, uint64_t startElem, uint64_t endElem,
	  uint64_t vecOffset, unsigned int* sizeMeta, char* buf) = 0;

  virtual const char* GetElement(uint64_t elementNr) = 0;
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
  virtual void Annotate(std::string annotation) = 0;
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
  virtual void Annotate(std::string annotation) = 0;
};


class ILogicalColumn
{
public:
  virtual ~ILogicalColumn() {};
  virtual int* Data() = 0;
};


#endif // IFST_COLUMN_H
