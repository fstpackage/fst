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


#ifndef FST_COLUMN_H
#define FST_COLUMN_H

#include <interface/ifstcolumn.h>
#include <fst_blockrunner_char.h>


class StringArray : public IStringArray
{
  SEXP strVec;
  bool isProtected;

public:

  ~StringArray() { if (isProtected) UNPROTECT(1); }

  void AllocateArray(unsigned int length)
  {
    PROTECT(this->strVec = Rf_allocVector(STRSXP, length));
    isProtected = true;
  }

  // Use an existing SEXP
  void SetArray(SEXP strVec)
  {
    this->strVec = strVec;
  }

  void SetElement(unsigned int elementNr, const char* str)
  {
    SET_STRING_ELT(strVec, elementNr, Rf_mkChar(str));
  }

  const char* GetElement(unsigned int elementNr)
  {
    return CHAR(STRING_ELT(strVec, elementNr));
  }

  void SetElement(unsigned int elementNr, const char* str, unsigned int strLen)
  {
    SET_STRING_ELT(strVec, elementNr, Rf_mkCharLen(str, strLen));
  }

  unsigned int Length()
  {
    return LENGTH(strVec);
  }

  SEXP StrVector() { return strVec; }
};


class FactorColumn : public IFactorColumn
{
public:
  SEXP intVec;
  std::unique_ptr<BlockReaderChar> blockReaderStrVecP;
  FstColumnAttribute columnAttribute;

  FactorColumn(int nrOfRows, FstColumnAttribute columnAttribute)
  {
    intVec = Rf_allocVector(INTSXP, nrOfRows);
    PROTECT(intVec);

    this->columnAttribute = columnAttribute;  // e.g. for 'FACTOR_ORDERED' specification
    blockReaderStrVecP = std::unique_ptr<BlockReaderChar>(new BlockReaderChar());
  }

  ~FactorColumn()
  {
    UNPROTECT(1);
  };

  int* LevelData()
  {
    return INTEGER(intVec);
  }

  IStringColumn* Levels()
  {
    return blockReaderStrVecP.get();
  }

  FstColumnAttribute Attribute()
  {
    return columnAttribute;
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


class Int64Column : public IInt64Column
{
public:
  SEXP int64Vec;

  Int64Column(int nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    int64Vec = Rf_allocVector(REALSXP, nrOfRows);
    PROTECT(int64Vec);

    // test for nanotime type
    if (columnAttribute == FstColumnAttribute::INT_64_TIME_SECONDS)
    {
      SEXP classAttr;

      if (scale != FstTimeScale::NANOSECONDS)
      {
        throw(std::runtime_error("Timestamp column with unknown scale detected"));
      }

      PROTECT(classAttr = Rf_mkString("nanotime"));
      Rf_setAttrib(classAttr, Rf_mkString("package"), Rf_mkString("nanotime"));

      UNPROTECT(1);  // necessary?
      Rf_classgets(int64Vec, classAttr);

      Rf_setAttrib(int64Vec, Rf_mkString(".S3Class"), Rf_mkString("integer64"));
      SET_S4_OBJECT(int64Vec);

      return;
    }

    // default int64 column type
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

  FstColumnAttribute ColumnAttribute()
  {
    return FstColumnAttribute::NONE;
  }
};


class DoubleColumn : public IDoubleColumn
{
  FstColumnAttribute columnAttribute;

  public:
    SEXP colVec;

    DoubleColumn(int nrOfRows, FstColumnAttribute columnAttribute, short int scale)
    {
      PROTECT(colVec = Rf_allocVector(REALSXP, nrOfRows));

      // store for later reference
      this->columnAttribute = columnAttribute;

      // difftime type
      if (columnAttribute == FstColumnAttribute::DOUBLE_64_TIMEINTERVAL_SECONDS)
      {
        Rf_classgets(colVec, Rf_mkString("difftime"));

        if (scale == (short int) FstTimeScale::SECONDS)
        {
          Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("secs"));
        }
        else if (scale == FstTimeScale::MINUTES)
        {
          Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("mins"));
        }
        else if (scale == FstTimeScale::HOURS)
        {
          Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("hours"));
        }
        else if (scale == FstTimeScale::DAYS)
        {
          Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("days"));
        }
        else
        {
          Rf_warning("Unknown time unit, defaulting to seconds");
          Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("secs"));
        }

        return;
      }

      // Date type
      if (columnAttribute == FstColumnAttribute::DOUBLE_64_DATE_DAYS)
      {
        Rf_classgets(colVec, Rf_mkString("Date"));
        return;
      }

      // POSIXct type
      if (columnAttribute == FstColumnAttribute::DOUBLE_64_TIMESTAMP_SECONDS)
      {
        SEXP classes;
        PROTECT(classes = Rf_allocVector(STRSXP, 2));
        SET_STRING_ELT(classes, 0, Rf_mkChar("POSIXct"));
        SET_STRING_ELT(classes, 1, Rf_mkChar("POSIXt"));
        UNPROTECT(1);

        Rf_classgets(colVec, classes);
        return;
      }
    }

    ~DoubleColumn()
    {
      UNPROTECT(1);
    }

    double* Data()
    {
      return REAL(colVec);
    }

    void Annotate(std::string annotation)
    {
      // annotation is used to set timezone. Empty annotation sets an empty time-zone
      if (this->columnAttribute == FstColumnAttribute::DOUBLE_64_TIMESTAMP_SECONDS)
      {
        if (annotation.length() > 0)
        {
          SEXP timeZone = Rf_ScalarString(Rf_mkCharLen(annotation.c_str(), annotation.length()));
          Rf_setAttrib(colVec, Rf_install("tzone"), timeZone);
          return;
        }

        Rf_setAttrib(colVec, Rf_install("tzone"), Rf_mkString(""));
        return;
      }
    }
};


class IntegerColumn : public IIntegerColumn
{
  FstColumnAttribute columnAttribute;

public:
  SEXP colVec;

  IntegerColumn(int nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    colVec = Rf_allocVector(INTSXP, nrOfRows);
    PROTECT(colVec);

    // store for later reference
    this->columnAttribute = columnAttribute;

    // set attributes if required
    if (columnAttribute == FstColumnAttribute::INT_32_TIMEINTERVAL_SECONDS)
    {
      Rf_classgets(colVec, Rf_mkString("difftime"));

      if (scale == FstTimeScale::SECONDS)
      {
        Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("secs"));
      }
      else if (scale == FstTimeScale::MINUTES)
      {
        Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("mins"));
      }
      else if (scale == FstTimeScale::HOURS)
      {
        Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("hours"));
      }
      else if (scale == FstTimeScale::DAYS)
      {
        Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("days"));
      }
      else
      {
        Rf_warning("Unknown time unit, defaulting to seconds");
        Rf_setAttrib(colVec, Rf_mkString("units"), Rf_mkString("secs"));
      }

      return;
    }

    if (columnAttribute == FstColumnAttribute::INT_32_DATE_DAYS)
    {
      SEXP classAttr;

      PROTECT(classAttr = Rf_allocVector(STRSXP, 2));
      SET_STRING_ELT(classAttr, 0, Rf_mkChar("IDate"));
      SET_STRING_ELT(classAttr, 1, Rf_mkChar("Date"));

      UNPROTECT(1);
      Rf_classgets(colVec, classAttr);

      return;
    }

    if (columnAttribute == FstColumnAttribute::INT_32_TIMESTAMP_SECONDS)
    {
      SEXP classes;
      PROTECT(classes = Rf_allocVector(STRSXP, 2));
      SET_STRING_ELT(classes, 0, Rf_mkChar("POSIXct"));
      SET_STRING_ELT(classes, 1, Rf_mkChar("POSIXt"));
      UNPROTECT(1);

      Rf_classgets(colVec, classes);
      return;
    }
  }

  ~IntegerColumn()
  {
    UNPROTECT(1);
  }

  int* Data()
  {
    return INTEGER(colVec);
  }

  void Annotate(std::string annotation)
  {
    // annotation is used to set timezone
    if (this->columnAttribute == FstColumnAttribute::INT_32_TIMESTAMP_SECONDS)
    {
      if (annotation.length() > 0)
      {
        Rf_setAttrib(colVec, Rf_install("tzone"),
          Rf_ScalarString(Rf_mkCharLen(annotation.c_str(), annotation.length())));
        return;
      }

      Rf_setAttrib(colVec, Rf_install("tzone"), Rf_mkString(""));
      return;
    }
  }
};


class ByteColumn : public IByteColumn
{
public:
  SEXP colVec;

  ByteColumn(int nrOfRows)
  {
    colVec = Rf_allocVector(RAWSXP, nrOfRows);
    PROTECT(colVec);
  }

  ~ByteColumn()
  {
    UNPROTECT(1);
  }

  char* Data()
  {
    return (char*) RAW(colVec);
  }
};


#endif  // FST_COLUMN_H
