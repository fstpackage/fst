/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  BSD 3-Clause (https://opensource.org/licenses/BSD-3-Clause)

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.

  3. Neither the name of the copyright holder nor the names of its
     contributors may be used to endorse or promote products derived from
     this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact the author at :
  - fstlib source repository : https://github.com/fstPackage/fstlib
*/


#ifndef IFST_TABLE_H
#define IFST_TABLE_H

#include "ifstcolumn.h"
#include "istringwriter.h"


/**
  Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
  The table only exists to facilitate serialization and deserialization of data.
*/
class IFstTable
{
  public:
    virtual ~IFstTable() {};

    virtual FstColumnType ColumnType(unsigned int colNr, FstColumnAttribute &columnAttribute, short int &scale,
      std::string &annotation, bool &hasAnnotation) = 0;

	// Writer interface
    virtual IStringWriter* GetStringWriter(unsigned int colNr) = 0;

    virtual int* GetLogicalWriter(unsigned int colNr) = 0;

    virtual int* GetIntWriter(unsigned int colNr) = 0;

  	virtual long long* GetInt64Writer(unsigned int colNr) = 0;

    virtual char* GetByteWriter(unsigned int colNr) = 0;

    virtual double* GetDoubleWriter(unsigned int colNr) = 0;

    virtual IStringWriter* GetLevelWriter(unsigned int colNr) = 0;

    virtual IStringWriter* GetColNameWriter() = 0;

    virtual void GetKeyColumns(int* keyColPos) = 0;

    virtual unsigned int NrOfKeys() = 0;

    virtual unsigned int NrOfColumns() = 0;

    virtual unsigned long long NrOfRows() = 0;

	// Reader interface
  	virtual void InitTable(unsigned int nrOfCols, unsigned long long nrOfRows) = 0;

  	virtual void SetStringColumn(IStringColumn* stringColumn, int colNr) = 0;

  	virtual void SetLogicalColumn(ILogicalColumn* logicalColumn, int colNr) = 0;

  	virtual void SetIntegerColumn(IIntegerColumn* integerColumn, int colNr, std::string &annotation) = 0;

    virtual void SetIntegerColumn(IIntegerColumn* integerColumn, int colNr) = 0;

  	virtual void SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr) = 0;

    virtual void SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr, std::string &annotation) = 0;

  	virtual void SetFactorColumn(IFactorColumn* factorColumn, int colNr) = 0;

  	virtual void SetInt64Column(IInt64Column* int64Column, int colNr) = 0;

  	virtual void SetByteColumn(IByteColumn* byteColumn, int colNr) = 0;

//  	virtual void SetColNames() = 0;

  	virtual void SetKeyColumns(int* keyColPos, unsigned int nrOfKeys) = 0;
};

#endif  // IFST_TABLE_H
