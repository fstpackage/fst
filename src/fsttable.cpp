/*
  fst - An R-package for ultra fast storage and retrieval of datasets.
  Header File
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



#include "ifsttable.h"
#include "fstdefines.h"

#include "blockrunner_char.h"
#include "fsttable.h"

#include <Rcpp.h>

using namespace Rcpp;


FstTable::FstTable(SEXP &table)
{
  this->rTable = &table;
}


unsigned int FstTable::NrOfColumns()
{
  if (nrOfCols == 0)
  {
    nrOfCols = Rf_length(*rTable);
  }

  return nrOfCols;
}


unsigned int FstTable::NrOfRows()
{
  if (nrOfCols == 0)  // table has zero columns
  {
    nrOfCols = Rf_length(*rTable);

    if (nrOfCols == 0) return 0;  // table has zero columns and rows
  }

  SEXP colVec = VECTOR_ELT(*rTable, 0);  // retrieve column vector
  return Rf_length(colVec);
}


FstColumnType FstTable::GetColumnType(unsigned int colNr)
{
  SEXP colVec = VECTOR_ELT(*rTable, colNr);  // retrieve column vector

  switch (TYPEOF(colVec))
  {
    case STRSXP:
      return FstColumnType::CHARACTER;

    case INTSXP:
      if(Rf_isFactor(colVec))
      {
        return FstColumnType::FACTOR;
      }

      return FstColumnType::INT_32;

    case REALSXP:
      return FstColumnType::DOUBLE_64;

    case LGLSXP:
      return FstColumnType::BOOL_32;

    default:
      return FstColumnType::UNKNOWN;
  }
}


int* FstTable::GetLogicalWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  return LOGICAL(cols);
}

int* FstTable::GetIntWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  return INTEGER(cols);
}

double* FstTable::GetDoubleWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  return REAL(cols);
}


IBlockWriter* FstTable::GetCharWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector

  // Assuming that nrOfRows is already set
  unsigned int nrOfVectorRows = LENGTH(cols);

  return new BlockWriterChar(cols, nrOfVectorRows, strSizes, naInts, buf, MAX_CHAR_STACK_SIZE);
}


IBlockWriter* FstTable::GetLevelWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  cols = Rf_getAttrib(cols, Rf_mkString("levels"));
  unsigned int nrOfFactorLevels = LENGTH(cols);
  return new BlockWriterChar(cols, nrOfFactorLevels, strSizes, naInts, buf, MAX_CHAR_STACK_SIZE);
}


IBlockWriter* FstTable::GetColNameWriter()
{
  cols = Rf_getAttrib(*rTable, R_NamesSymbol);

  // Assuming that nrOfCols is already set
  return new BlockWriterChar(cols, nrOfCols, strSizes, naInts, buf, MAX_CHAR_STACK_SIZE);
}


inline unsigned int FindKey(StringVector colNameList, String item)
{
  int index = -1;
  int found = 0;
  for (Rcpp::StringVector::iterator it = colNameList.begin(); it != colNameList.end(); ++it)
  {
    if (*it == item)
    {
      index = found;
      break;
    }
    ++found;
  }

  return index;
}


unsigned int FstTable::NrOfKeys()
{
  SEXP keyNames = Rf_getAttrib(*rTable, Rf_mkString("sorted"));
  if (Rf_isNull(keyNames)) return 0;

  return LENGTH(keyNames);
}


void FstTable::GetKeyColumns(int* keyColPos)
{
  SEXP keyNames = Rf_getAttrib(*rTable, Rf_mkString("sorted"));
  if (Rf_isNull(keyNames)) return;

  int keyLength = LENGTH(keyNames);

  // Find key column index numbers, if any
  StringVector keyList(keyNames);
  SEXP colNames = Rf_getAttrib(*rTable, R_NamesSymbol);

  for (int colSel = 0; colSel < keyLength; ++colSel)
  {
    keyColPos[colSel] = FindKey(colNames, keyList[colSel]);
  }
}

