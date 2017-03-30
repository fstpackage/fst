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


#ifndef FST_TABLE_H
#define FST_TABLE_H

#include <iostream>
#include <vector>


enum FstColumnType
{
  INT_32,
  BOOL_32,
  DOUBLE_64,
  CHARACTER,
  FACTOR
};


class FstColumn
{
public:
  FstColumnType colType;

  // virtual void Serialize(std::ostream fstStream);
  // virtual void DeSerialize(std::istream fstStream);

  virtual ~FstColumn() = 0;
};


class FstZipper
{
  public:
    FstZipper();
};


class FstColumn_int32 : public FstColumn
{
  int* colData;  // buffer lifetime is managed outside the fst framework

  public:
    ~FstColumn_int32() {}  // cleanup

    FstColumn_int32(int* colData, uint64_t colSize)
    {
      colType = FstColumnType::INT_32;
    }

    // Select algorithm here
    FstColumn_int32(int* colData, uint64_t colSize, int minValue, int maxValue)
    {
      colType = FstColumnType::INT_32;
    }
};


/**
  Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
  The table only exists to facilitate serialization and deserialization of data.
*/
class FstTable
{
  uint64_t nrOfRows;
  std::vector<FstColumn*> columns;

  public:
    FstTable(uint64_t nrOfRows);
    ~FstTable();

    // Access to private members
    const std::vector<FstColumn*>& Columns() const { return columns; }
    uint64_t NrOfRows() { return nrOfRows; }

    // Add columns of specific types
    void AddColumnInt32(int* colData);
    void AddColumnInt32(int* colData, int minValue, int maxValue);  // helpers for compression
};


#endif  // FST_TABLE_H
