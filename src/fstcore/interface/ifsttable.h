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


#ifndef IFST_TABLE_H
#define IFST_TABLE_H

#include "ifstcolumn.h"
#include "iblockrunner.h"

enum FstColumnType
{
  CHARACTER = 1,
  FACTOR,
  INT_32,
  DOUBLE_64,
  BOOL_32,
  UNKNOWN
};


/**
  Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
  The table only exists to facilitate serialization and deserialization of data.
*/
class IFstTable
{
  public:
    virtual ~IFstTable() {};

    virtual FstColumnType GetColumnType(unsigned int colNr) = 0;

    virtual IBlockWriter* GetCharWriter(unsigned int colNr) = 0;

    virtual int* GetLogicalWriter(unsigned int colNr) = 0;

    virtual int* GetIntWriter(unsigned int colNr) = 0;

    virtual double* GetDoubleWriter(unsigned int colNr) = 0;

    virtual IBlockWriter* GetLevelWriter(unsigned int colNr) = 0;

    virtual IBlockWriter* GetColNameWriter() = 0;

    virtual void GetKeyColumns(int* keyColPos) = 0;

    virtual unsigned int NrOfKeys() = 0;

    virtual unsigned int NrOfColumns() = 0;

    virtual unsigned int NrOfRows() = 0;
};


/**
 Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
 The table only exists to facilitate serialization and deserialization of data.
 */
class IFstTableReader
{
public:
  virtual ~IFstTableReader() {};

  virtual void InitTable(unsigned int nrOfCols, int nrOfRows) = 0;

  virtual void AddCharColumn(IBlockReader* stringColumn, int colNr) = 0;

  virtual void AddLogicalColumn(ILogicalColumn* logicalColumn, int colNr) = 0;

  virtual int* AddIntColumn(int colNr) = 0;

  virtual void AddDoubleColumn(IDoubleColumn* doubleColumn, int colNr) = 0;

  virtual void AddFactorColumn(IFactorColumn* factorColumn, int colNr) = 0;

  virtual void SetColNames() = 0;

  virtual void SetKeyColumns(int* keyColPos, unsigned int nrOfKeys) = 0;

  virtual unsigned int NrOfColumns() = 0;

  virtual unsigned int NrOfRows() = 0;
};

#endif  // IFST_TABLE_H
