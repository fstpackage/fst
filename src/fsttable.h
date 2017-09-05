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

#include <interface/fstdefines.h>
#include <interface/istringwriter.h>
#include <interface/ifsttable.h>


/**
  Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
  The table only exists to facilitate serialization and deserialization of data.
*/
class FstTable : public IFstTable
{
  // References to R objects
  SEXP* rTable;  // reference to R table structure (e.g. data.frame or data.table)
  SEXP  cols;  // reference to working column

  // Table metadata
  unsigned int nrOfCols;
  int nrOfRows;
  bool isProtected;
  int uniformEncoding;


  public:
    SEXP resTable;

    FstTable() {}

    FstTable(SEXP &table, int uniformEncoding);

    ~FstTable() { if (isProtected) UNPROTECT(1); }

    void InitTable(unsigned int nrOfCols, int nrOfRows);

    void SetStringColumn(IStringColumn* stringColumn, int colNr);

    void SetLogicalColumn(ILogicalColumn* logicalColumn, int colNr);

    void SetIntegerColumn(IIntegerColumn* integerColumn, int colNr);

    void SetByteColumn(IByteColumn* byteColumn, int colNr);

    void SetDateTimeColumn(IDateTimeColumn* dateTimeColumn, int colNr);

    void SetInt64Column(IInt64Column* int64Column, int colNr);

    void SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr);

    void SetFactorColumn(IFactorColumn* factorColumn, int colNr);

    void SetColNames();

    void SetKeyColumns(int* keyColPos, unsigned int nrOfKeys);

    FstColumnType ColumnType(unsigned int colNr, FstColumnAttribute &columnAttribute);

    IStringWriter* GetStringWriter(unsigned int colNr);

    // void AddCharColumn(IBlockReader* stringColumn, int colNr);

    int* GetLogicalWriter(unsigned int colNr);

    int* GetIntWriter(unsigned int colNr);

    char* GetByteWriter(unsigned int colNr);

    int* GetDateTimeWriter(unsigned int colNr);

    long long* GetInt64Writer(unsigned int colNr);

    double* GetDoubleWriter(unsigned int colNr);

    IStringWriter* GetLevelWriter(unsigned int colNr);

    IStringWriter* GetColNameWriter();

    void GetKeyColumns(int* keyColPos);

    unsigned int NrOfKeys();

    unsigned int NrOfColumns();

    unsigned int NrOfRows();
};


#endif  // FST_TABLE_H
