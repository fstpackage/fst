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


#include <Rcpp.h>

#include <interface/ifsttable.h>
#include <interface/istringwriter.h>
#include <interface/fstdefines.h>

#include <blockrunner_char.h>
#include <fstcolumn.h>

#include <fsttable.h>

using namespace Rcpp;


FstTable::FstTable(SEXP &table, int uniformEncoding)
{
  this->rTable = &table;
  this->nrOfCols = 0;
  this->isProtected = false;
  this->uniformEncoding = uniformEncoding;
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


FstColumnType FstTable::ColumnType(unsigned int colNr)
{
  SEXP colVec = VECTOR_ELT(*rTable, colNr);  // retrieve column vector

  switch (TYPEOF(colVec))
  {
    case STRSXP:
      return FstColumnType::CHARACTER;

    case INTSXP:
      if (Rf_isFactor(colVec))  // factor
      {
        return FstColumnType::FACTOR;
      }
      else if (Rf_inherits(colVec, "Date"))
      {
        return FstColumnType::DATE_INT;
      }

      return FstColumnType::INT_32;

    case REALSXP:
      if (Rf_inherits(colVec, "Date"))
      {
        return FstColumnType::DATE_INT;
      }

      if (Rf_inherits(colVec, "integer64"))
      {
        return FstColumnType::INT_64;
      }

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


int* FstTable::GetDateTimeWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector (no copy?)

  // Convert underlying type to INTSXP
  if (TYPEOF(cols) == REALSXP)
  {
    cols = Rf_coerceVector(cols, INTSXP);  // PROTECT needed here?: if so use list container
  }

  return INTEGER(cols);
}


long long* FstTable::GetInt64Writer(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector (no copy?)

  // Convert doubles to long integer type
  return (long long*) (REAL(cols));
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


IStringWriter* FstTable::GetStringWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector

  // Assuming that nrOfRows is already set
  unsigned int nrOfVectorRows = LENGTH(cols);

  return new BlockWriterChar(cols, nrOfVectorRows, MAX_CHAR_STACK_SIZE, uniformEncoding);
}


IStringWriter* FstTable::GetLevelWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  cols = Rf_getAttrib(cols, Rf_mkString("levels"));
  unsigned int nrOfFactorLevels = LENGTH(cols);
  return new BlockWriterChar(cols, nrOfFactorLevels, MAX_CHAR_STACK_SIZE, uniformEncoding);
}


IStringWriter* FstTable::GetColNameWriter()
{
  cols = Rf_getAttrib(*rTable, R_NamesSymbol);

  // Assuming that nrOfCols is already set
  return new BlockWriterChar(cols, nrOfCols, MAX_CHAR_STACK_SIZE, uniformEncoding);
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


//  FstTableReader implementation

void FstTable::InitTable(unsigned int nrOfCols, int nrOfRows)
{
  this->nrOfCols = nrOfCols;
  this->nrOfRows = nrOfRows;

  this->resTable = Rf_allocVector(VECSXP, nrOfCols);
  PROTECT(resTable);
  isProtected = true;
}


void FstTable::SetStringColumn(IStringColumn* stringColumn, int colNr)
{
  BlockReaderChar* sColumn = (BlockReaderChar*) stringColumn;
  SET_VECTOR_ELT(resTable, colNr, sColumn->StrVector());
}


void FstTable::SetLogicalColumn(ILogicalColumn* logicalColumn, int colNr)
{
  LogicalColumn* lColumn = (LogicalColumn*) logicalColumn;
  SET_VECTOR_ELT(resTable, colNr, lColumn->boolVec);
}


void FstTable::SetDateTimeColumn(IDateTimeColumn* dateTimeColumn, int colNr)
{
  DateTimeColumn* dTimeColumn = (DateTimeColumn*) dateTimeColumn;
  SET_VECTOR_ELT(resTable, colNr, dTimeColumn->dateTimeVec);
}


void FstTable::SetInt64Column(IInt64Column* int64Column, int colNr)
{
  Int64Column* i64Column = (Int64Column*) int64Column;
  SET_VECTOR_ELT(resTable, colNr, i64Column->int64Vec);
}


void FstTable::SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr)
{
  DoubleColumn* dColumn = (DoubleColumn*) doubleColumn;
  SET_VECTOR_ELT(resTable, colNr, dColumn->colVec);
}


void FstTable::SetIntegerColumn(IIntegerColumn* integerColumn, int colNr)
{
  IntegerColumn* iColumn = (IntegerColumn*) integerColumn;
  SET_VECTOR_ELT(resTable, colNr, iColumn->colVec);
}


void FstTable::SetFactorColumn(IFactorColumn* factorColumn, int colNr)
{
  Rf_setAttrib(((FactorColumn*) factorColumn)->intVec, Rf_mkString("levels"), ((FactorColumn*) factorColumn)->blockReaderStrVec->StrVector());
  Rf_setAttrib(((FactorColumn*) factorColumn)->intVec, Rf_mkString("class"), Rf_mkString("factor"));

  SET_VECTOR_ELT(resTable, colNr, ((FactorColumn*) factorColumn)->intVec);
}


void FstTable::SetColNames()
{
  // BlockReaderChar* blockReader = new BlockReaderChar();
  // return blockReader;
}

void FstTable::SetKeyColumns(int* keyColPos, unsigned int nrOfKeys)
{

}

