/*
 fst - R package for ultra fast storage and retrieval of datasets

 Copyright (C) 2017-present, Mark AJ Klik

 This file is part of the fst R package.

 The fst R package is free software: you can redistribute it and/or modify it
 under the terms of the GNU Affero General Public License version 3 as
 published by the Free Software Foundation.

 The fst R package is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 for more details.

 You should have received a copy of the GNU Affero General Public License along
 with the fst R package. If not, see <http://www.gnu.org/licenses/>.

 You can contact the author at:
 - fst R package source repository : https://github.com/fstpackage/fst
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
  SEXP cols;  // reference to working column

  // container for r objects, must be a VECSXP of length >= 1
  SEXP r_container;

  // Table metadata
  unsigned int nrOfCols;
  unsigned long long nrOfRows;
  int uniformEncoding;


  public:
    // use r_container for res_table
    // SEXP resTable;

    FstTable(SEXP r_container)
    {
      this->r_container = r_container;
    }

    FstTable(SEXP &table, int uniformEncoding, SEXP r_container);

    ~FstTable() {}

    SEXP ResTable() { return VECTOR_ELT(r_container, 0); }

    void InitTable(unsigned int nrOfCols, unsigned long long nrOfRows);

    void SetStringColumn(IStringColumn* stringColumn, int colNr);

    void SetLogicalColumn(ILogicalColumn* logicalColumn, int colNr);

    void SetIntegerColumn(IIntegerColumn* integerColumn, int colNr);

    // void SetIntegerColumn(IIntegerColumn* integerColumn, int colNr, std::string &annotation);

    void SetByteColumn(IByteColumn* byteColumn, int colNr);

    void SetInt64Column(IInt64Column* int64Column, int colNr);

  	void SetDoubleColumn(IDoubleColumn * doubleColumn, int colNr);

  	IByteBlockColumn* add_byte_block_column(unsigned col_nr);

  	void SetFactorColumn(IFactorColumn* factorColumn, int colNr);

    void SetColNames();

    void SetKeyColumns(int* keyColPos, unsigned int nrOfKeys);

    FstColumnType ColumnType(unsigned int colNr, FstColumnAttribute &columnAttribute, short int &scale,
      std::string &annotation, bool &hasAnnotation);

    IStringWriter* GetStringWriter(unsigned int colNr);

    int* GetLogicalWriter(unsigned int colNr);

    int* GetIntWriter(unsigned int colNr);

    char* GetByteWriter(unsigned int colNr);

    long long* GetInt64Writer(unsigned int colNr);

    double* GetDoubleWriter(unsigned int colNr);

    IByteBlockColumn* GetByteBlockWriter(unsigned int col_nr);

    IStringWriter* GetLevelWriter(unsigned int colNr);

    IStringWriter* GetColNameWriter();

    void GetKeyColumns(int* keyColPos);

    unsigned int NrOfKeys();

    unsigned int NrOfColumns();

    void SetColNames(IStringArray* col_names);

    SEXP GetColNames();

    unsigned long long NrOfRows();
};


#endif  // FST_TABLE_H
