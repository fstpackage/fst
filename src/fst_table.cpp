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


#include <Rcpp.h>

#include <interface/ifsttable.h>
#include <interface/istringwriter.h>
#include <interface/fstdefines.h>

#include <fst_blockrunner_char.h>
#include <fst_column.h>
#include <fst_table.h>

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


unsigned long long FstTable::NrOfRows()
{
  if (nrOfCols == 0)  // table has zero columns
  {
    nrOfCols = Rf_length(*rTable);

    if (nrOfCols == 0) return 0;  // table has zero columns and rows
  }

  SEXP colVec = VECTOR_ELT(*rTable, 0);  // retrieve column vector
  return XLENGTH(colVec);
}


FstColumnType FstTable::ColumnType(unsigned int colNr, FstColumnAttribute &columnAttribute, short int &scale,
  std::string &annotation, bool &hasAnnotation)
{
  SEXP colVec = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  hasAnnotation = false;

  switch (TYPEOF(colVec))
  {
    case STRSXP:
      columnAttribute = FstColumnAttribute::CHARACTER_BASE;
      return FstColumnType::CHARACTER;

    case INTSXP:
      if (Rf_inherits(colVec, "difftime"))
      {
        columnAttribute = FstColumnAttribute::INT_32_TIMEINTERVAL_SECONDS;

        // Convert units
        SEXP units = Rf_getAttrib(colVec, Rf_install("units"));
        std::string unit = std::string(CHAR(STRING_ELT(units, 0)));

        if (unit == "secs")
        {
          scale = FstTimeScale::SECONDS;
        }
        else if (unit == "mins")
        {
          scale = FstTimeScale::MINUTES;
        }
        else if (unit == "hours")
        {
          scale = FstTimeScale::HOURS;
        }
        else if (unit == "days")
        {
          scale = FstTimeScale::DAYS;
        }
        else
        {
          Rf_warning("Unknown time unit, defaulting to seconds");
          scale = FstTimeScale::SECONDS;
        }

        return FstColumnType::INT_32;
      }

      if (Rf_isFactor(colVec))  // factor
      {
        if (Rf_inherits(colVec, "ordered"))
        {
          columnAttribute = FstColumnAttribute::FACTOR_ORDERED;
          return FstColumnType::FACTOR;
        }

        columnAttribute = FstColumnAttribute::FACTOR_BASE;
        return FstColumnType::FACTOR;
      }

      if (Rf_inherits(colVec, "Date"))
      {
        columnAttribute = FstColumnAttribute::INT_32_DATE_DAYS;
        return FstColumnType::INT_32;
      }

      if (Rf_inherits(colVec, "ITime"))
      {
        columnAttribute = FstColumnAttribute::INT_32_TIME_OF_DAY_SECONDS;
        scale = FstTimeScale::SECONDS;
        return FstColumnType::INT_32;
      }

      if (Rf_inherits(colVec, "POSIXct"))
      {
        hasAnnotation = false;
        columnAttribute = FstColumnAttribute::INT_32_TIMESTAMP_SECONDS;

        SEXP tzoneR = Rf_getAttrib(colVec, Rf_install("tzone"));

        if (!Rf_isNull(tzoneR))
        {
          hasAnnotation = true;
          annotation += Rf_translateCharUTF8(STRING_ELT(tzoneR, 0));
        }

        return FstColumnType::INT_32;
      }

      columnAttribute = FstColumnAttribute::INT_32_BASE;
      return FstColumnType::INT_32;

    case REALSXP:
      if (Rf_inherits(colVec, "ITime"))
      {
        columnAttribute = FstColumnAttribute::DOUBLE_64_TIME_OF_DAY_SECONDS;
        scale = FstTimeScale::SECONDS;
        return FstColumnType::DOUBLE_64;
      }

      if (Rf_inherits(colVec, "difftime"))
      {
        columnAttribute = FstColumnAttribute::DOUBLE_64_TIMEINTERVAL_SECONDS;

        // Convert units
        SEXP units = Rf_getAttrib(colVec, Rf_install("units"));
        std::string unit = std::string(CHAR(STRING_ELT(units, 0)));

        if (unit == "secs")
        {
          scale = FstTimeScale::SECONDS;
        }
        else if (unit == "mins")
        {
          scale = FstTimeScale::MINUTES;
        }
        else if (unit == "hours")
        {
          scale = FstTimeScale::HOURS;
        }
        else if (unit == "days")
        {
          scale = FstTimeScale::DAYS;
        }
        else
        {
          Rf_warning("Unknown time unit, defaulting to seconds");
          scale = FstTimeScale::SECONDS;
        }

        return FstColumnType::DOUBLE_64;
      }

      if (Rf_inherits(colVec, "Date"))
      {
        columnAttribute = FstColumnAttribute::DOUBLE_64_DATE_DAYS;
        return FstColumnType::DOUBLE_64;
      }

      if (Rf_inherits(colVec, "POSIXct"))
      {
        hasAnnotation = false;
        columnAttribute = FstColumnAttribute::DOUBLE_64_TIMESTAMP_SECONDS;

        SEXP tzoneR = Rf_getAttrib(colVec, Rf_install("tzone"));

        if (!Rf_isNull(tzoneR))
        {
          hasAnnotation = true;
          annotation += Rf_translateCharUTF8(STRING_ELT(tzoneR, 0));
        }

        return FstColumnType::DOUBLE_64;
      }

      if (Rf_inherits(colVec, "nanotime"))
      {
        columnAttribute = FstColumnAttribute::INT_64_TIME_SECONDS;

        scale = FstTimeScale::NANOSECONDS;  // set scale to nanoseconds

        return FstColumnType::INT_64;
      }

      if (Rf_inherits(colVec, "integer64"))
      {
        columnAttribute = FstColumnAttribute::INT_64_BASE;
        return FstColumnType::INT_64;
      }

      columnAttribute = FstColumnAttribute::DOUBLE_64_BASE;
      return FstColumnType::DOUBLE_64;

    case LGLSXP:
      columnAttribute = FstColumnAttribute::BOOL_2_BASE;
      return FstColumnType::BOOL_2;

    case RAWSXP:
      columnAttribute = FstColumnAttribute::BYTE_BASE;
      return FstColumnType::BYTE;

    default:
      columnAttribute = FstColumnAttribute::NONE;
      return FstColumnType::UNKNOWN;
  }
}


int* FstTable::GetLogicalWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  return LOGICAL(cols);
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


char* FstTable::GetByteWriter(unsigned int colNr)
{
  cols = VECTOR_ELT(*rTable, colNr);  // retrieve column vector
  return (char*) RAW(cols);
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

void FstTable::InitTable(unsigned int nrOfCols, unsigned long long nrOfRows)
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


void FstTable::SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr, std::string &annotation)
{
  DoubleColumn* dColumn = (DoubleColumn*) doubleColumn;
  dColumn->Annotate(annotation);
  SET_VECTOR_ELT(resTable, colNr, dColumn->colVec);
}


void FstTable::SetIntegerColumn(IIntegerColumn* integerColumn, int colNr)
{
  IntegerColumn* iColumn = (IntegerColumn*) integerColumn;
  SET_VECTOR_ELT(resTable, colNr, iColumn->colVec);
}


void FstTable::SetIntegerColumn(IIntegerColumn* integerColumn, int colNr, std::string &annotation)
{
  IntegerColumn* iColumn = (IntegerColumn*) integerColumn;
  iColumn->Annotate(annotation);
  SET_VECTOR_ELT(resTable, colNr, iColumn->colVec);
}


void FstTable::SetByteColumn(IByteColumn* byteColumn, int colNr)
{
  ByteColumn* bColumn = (ByteColumn*) byteColumn;
  SET_VECTOR_ELT(resTable, colNr, bColumn->colVec);
}


void FstTable::SetFactorColumn(IFactorColumn* factorColumn, int colNr)
{
  FactorColumn* factColumn = (FactorColumn*) factorColumn;
  Rf_setAttrib(factColumn->intVec, Rf_mkString("levels"), factColumn->blockReaderStrVecP->StrVector());

  if (factColumn->Attribute() == FstColumnAttribute::FACTOR_ORDERED)  // ordered factor
  {
    SEXP classes;
    PROTECT(classes = Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(classes, 0, Rf_mkChar("ordered"));
    SET_STRING_ELT(classes, 1, Rf_mkChar("factor"));
    Rf_setAttrib(factColumn->intVec, Rf_mkString("class"), classes);
    UNPROTECT(1);
  }
  else  // unordered factor
  {
    Rf_setAttrib(factColumn->intVec, Rf_mkString("class"), Rf_mkString("factor"));
  }

  SET_VECTOR_ELT(resTable, colNr, factColumn->intVec);
}


void FstTable::SetColNames()
{
  // BlockReaderChar* blockReader = new BlockReaderChar();
  // return blockReader;
}

void FstTable::SetKeyColumns(int* keyColPos, unsigned int nrOfKeys)
{

}

