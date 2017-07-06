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


#ifndef FST_COLUMN_H
#define FST_COLUMN_H

#include <interface/ifstcolumn.h>
#include <blockrunner_char.h>


class StringArray : public IStringArray
{
  SEXP strVec;
  bool isProtected;

public:

  ~StringArray() { if (isProtected) UNPROTECT(1); }

  void AllocateArray(int length)
  {
    PROTECT(this->strVec = Rf_allocVector(STRSXP, length));
    isProtected = true;
  }

  // Use an existing SEXP
  void SetArray(SEXP strVec)
  {
    this->strVec = strVec;
  }

  void SetElement(int elementNr, const char* str)
  {
    SET_STRING_ELT(strVec, elementNr, Rf_mkChar(str));
  }

  const char* GetElement(int elementNr)
  {
    return CHAR(STRING_ELT(strVec, elementNr));
  }

  void SetElement(int elementNr, const char* str, int strLen)
  {
    SET_STRING_ELT(strVec, elementNr, Rf_mkCharLen(str, strLen));
  }

  int Length()
  {
    return LENGTH(strVec);
  }

  SEXP StrVector() { return strVec; }
};


class FactorColumn : public IFactorColumn
{
public:
  SEXP intVec;
  BlockReaderChar* blockReaderStrVec;

  FactorColumn(int nrOfRows)
  {
    intVec = Rf_allocVector(INTSXP, nrOfRows);
    PROTECT(intVec);
    blockReaderStrVec = new BlockReaderChar();
  }

  ~FactorColumn()
  {
    UNPROTECT(1);
    delete blockReaderStrVec;
  };

  int* LevelData()
  {
    return INTEGER(intVec);
  }

  IStringColumn* Levels()
  {
    return blockReaderStrVec;
  }
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


class DateTimeColumn : public IDateTimeColumn
{
public:
  SEXP dateTimeVec;

  DateTimeColumn(int nrOfRows)
  {
    dateTimeVec = Rf_allocVector(INTSXP, nrOfRows);
    PROTECT(dateTimeVec);

    Rf_setAttrib(dateTimeVec, Rf_mkString("class"), Rf_mkString("Date"));
  }

  ~DateTimeColumn()
  {
    UNPROTECT(1);
  }

  int* Data()
  {
    return INTEGER(dateTimeVec);
  }
};


class Int64Column : public IInt64Column
{
public:
  SEXP int64Vec;

  Int64Column(int nrOfRows)
  {
    int64Vec = Rf_allocVector(REALSXP, nrOfRows);
    PROTECT(int64Vec);

    Rf_setAttrib(int64Vec, Rf_mkString("class"), Rf_mkString("integer64"));
  }

  ~Int64Column()
  {
    UNPROTECT(1);
  }

  long long* Data()
  {
    return (long long*)(REAL(int64Vec));
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


class IntegerColumn : public IIntegerColumn
{
public:
  SEXP colVec;

  IntegerColumn(int nrOfRows)
  {
    colVec = Rf_allocVector(INTSXP, nrOfRows);
    PROTECT(colVec);
  }

  ~IntegerColumn()
  {
    UNPROTECT(1);
  }

  int* Data()
  {
    return INTEGER(colVec);
  }
};


#endif  // FST_COLUMN_H
