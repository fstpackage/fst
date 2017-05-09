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

#include <Rcpp.h>

#include <fstdefines.h>
#include <iblockrunner.h>
#include <ifsttable.h>


/**
  Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
  The table only exists to facilitate serialization and deserialization of data.
*/
class FstTable : public IFstTable
{
  // References to R objects
  SEXP* rTable;
  SEXP  cols;
  SEXP levelVec;

  // Buffers for blockRunner
  unsigned int naInts[1 + BLOCKSIZE_CHAR / 32];  // we have 32 NA bits per integer
  unsigned int strSizes[BLOCKSIZE_CHAR];  // we have 32 NA bits per integer
  char buf[MAX_CHAR_STACK_SIZE];

  // Table metadata
  unsigned int nrOfCols;

  public:
    FstTable(SEXP &table);
    ~FstTable() {};

    FstColumnType GetColumnType(unsigned int colNr);

    IBlockWriter* GetCharWriter(unsigned int colNr);

    int* GetLogicalWriter(unsigned int colNr);

    int* GetIntWriter(unsigned int colNr);

    double* GetDoubleWriter(unsigned int colNr);

    IBlockWriter* GetLevelWriter(unsigned int colNr);

    IBlockWriter* GetColNameWriter();

    void GetKeyColumns(int* keyColPos);

    unsigned int NrOfKeys();

    unsigned int NrOfColumns();

    unsigned int NrOfRows();
};


class LogicalColumn : public ILogicalColumn
{
public:
  SEXP boolVec;

  LogicalColumn(int nrOfRows)
  {
    boolVec = Rf_allocVector(LGLSXP, nrOfRows);
    PROTECT(boolVec);
  }

  ~LogicalColumn()
  {
    UNPROTECT(1);
  }

  int* Data()
  {
    return LOGICAL(boolVec);
  }
};


class DoubleColumn : public IDoubleColumn
{
  public:
    SEXP colVec;

    DoubleColumn(int nrOfRows)
    {
      colVec = Rf_allocVector(REALSXP, nrOfRows);
      PROTECT(colVec);
    }

    ~DoubleColumn()
    {
      UNPROTECT(1);
    }

    double* Data()
    {
      return REAL(colVec);
    }
};


class FstTableReader : public IFstTableReader
{
  // Buffers for blockRunner
  unsigned int naInts[1 + BLOCKSIZE_CHAR / 32];  // we have 32 NA bits per integer
  unsigned int strSizes[BLOCKSIZE_CHAR];  // we have 32 NA bits per integer
  char buf[MAX_CHAR_STACK_SIZE];

  // Table metadata
  unsigned int nrOfCols;
  int nrOfRows;

public:
  // Result table
  SEXP resTable;
  SEXP colVec;

  void InitTable(unsigned int nrOfCols, int nrOfRows);

  ~FstTableReader() {};

  IBlockReader* GetCharReader();

  void AddLogicalColumn(ILogicalColumn* logicalColumn, int colNr);

  int* AddIntColumn(int colNr);

  void AddDoubleColumn(IDoubleColumn* doubleColumn, int colNr);

  IBlockReader* GetLevelReader();

  void SetColNames();

  void SetKeyColumns(int* keyColPos, unsigned int nrOfKeys);

  unsigned int NrOfColumns();

  unsigned int NrOfRows();
};


#endif  // FST_TABLE_H
