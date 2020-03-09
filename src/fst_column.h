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
  cetype_t strEncoding = cetype_t::CE_NATIVE;

public:

  ~StringArray() { }

  // strVec should be PROTECTED after calling this method
  void AllocateArray(uint64_t length)
  {
    this->strVec = Rf_allocVector(STRSXP, (R_xlen_t) length);
  }

  // Use an existing SEXP
  void SetArray(SEXP strVec)
  {
    this->strVec = strVec;
  }

  // set the string encoding to a new value
  void SetEncoding(StringEncoding string_encoding)
  {
    switch (string_encoding)
    {
      case StringEncoding::LATIN1:
      {
        this->strEncoding = cetype_t::CE_LATIN1;
        break;
      }

      case StringEncoding::UTF8:
      {
        this->strEncoding = cetype_t::CE_UTF8;
        break;
      }

      default:  // native or unknown encoding
        this->strEncoding = cetype_t::CE_NATIVE;
        break;
    }
  }

  void SetElement(uint64_t elementNr, const char* str)
  {
    // use current string encoding
    SEXP str_elem = Rf_mkCharLenCE(str, strlen(str), strEncoding);
    SET_STRING_ELT(strVec, elementNr, str_elem);
  }

  const char* GetElement(uint64_t elementNr)
  {
    return CHAR(STRING_ELT(strVec, elementNr));
  }

  uint64_t Length()
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
  // FstColumnAttribute columnAttribute;

  FactorColumn(uint64_t nrOfRows, uint64_t nrOfLevels, FstColumnAttribute columnAttribute)
  {
    intVec = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t) nrOfRows));

    // this->columnAttribute = columnAttribute;  // e.g. for 'FACTOR_ORDERED' specification
    blockReaderStrVecP = std::unique_ptr<BlockReaderChar>(new BlockReaderChar());

    BlockReaderChar* block_reader = blockReaderStrVecP.get();
    block_reader->AllocateVec(nrOfLevels);
    SEXP str_vector = PROTECT(block_reader->StrVector());

    // set and PROTECT the levels attribute
    SEXP level_str = PROTECT(Rf_mkString("levels"));
    Rf_setAttrib(intVec, level_str, str_vector);
    UNPROTECT(2); // level_str, str_vector

    if (columnAttribute == FstColumnAttribute::FACTOR_ORDERED)  // ordered factor
    {
      SEXP class_str = PROTECT(Rf_mkString("class"));
      SEXP classes = PROTECT(Rf_allocVector(STRSXP, 2));

      SET_STRING_ELT(classes, 0, Rf_mkChar("ordered"));
      SET_STRING_ELT(classes, 1, Rf_mkChar("factor"));
      Rf_setAttrib(intVec, class_str, classes);

      UNPROTECT(2);  // classes, class_str
    }
    else  // unordered factor
    {
      SEXP class_str = PROTECT(Rf_mkString("class"));
      SEXP factor_str = PROTECT(Rf_mkString("factor"));

      Rf_setAttrib(intVec, class_str, factor_str);

      UNPROTECT(2);  // factor_str, class_str
    }

    UNPROTECT(1);  // intVec
  }

  ~FactorColumn()
  {
  };

  int* LevelData()
  {
    return INTEGER(intVec);
  }

  IStringColumn* Levels()
  {
    return blockReaderStrVecP.get();
  }
};


class LogicalColumn : public ILogicalColumn
{
public:
  SEXP boolVec;

  LogicalColumn(uint64_t nrOfRows)
  {
    boolVec = Rf_allocVector(LGLSXP, (R_xlen_t) nrOfRows);
  }

  ~LogicalColumn()
  {
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

  Int64Column(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    int64Vec = PROTECT(Rf_allocVector(REALSXP, (R_xlen_t) nrOfRows));

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

      Rf_classgets(int64Vec, classAttr);

      Rf_setAttrib(int64Vec, Rf_mkString(".S3Class"), Rf_mkString("integer64"));
      SET_S4_OBJECT(int64Vec);

      UNPROTECT(2);  // int64Vec, classAttr
      return;
    }

    // default int64 column type
    SEXP int64_str = PROTECT(Rf_mkString("integer64"));

    Rf_classgets(int64Vec, int64_str);

    UNPROTECT(2);  // int64_str, int64Vec
  }

  ~Int64Column()
  {
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

    DoubleColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
    {
      PROTECT(colVec = Rf_allocVector(REALSXP, (R_xlen_t) nrOfRows));

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

        UNPROTECT(1); // colVec
        return;
      }

      // Date type
      if (columnAttribute == FstColumnAttribute::DOUBLE_64_DATE_DAYS)
      {
        Rf_classgets(colVec, Rf_mkString("Date"));
        UNPROTECT(1); // colVec
        return;
      }

      // ITime type
      if (columnAttribute == FstColumnAttribute::DOUBLE_64_TIME_OF_DAY_SECONDS)
      {
        Rf_classgets(colVec, Rf_mkString("ITime"));

        if (scale != FstTimeScale::SECONDS)
        {
          UNPROTECT(1); // colVec
          throw(std::runtime_error("ITime column with unknown scale detected"));
        }

        UNPROTECT(1); // colVec
        return;
      }


      // POSIXct type
      if (columnAttribute == FstColumnAttribute::DOUBLE_64_TIMESTAMP_SECONDS)
      {
        SEXP classes = PROTECT(Rf_allocVector(STRSXP, 2));

        SET_STRING_ELT(classes, 0, Rf_mkChar("POSIXct"));
        SET_STRING_ELT(classes, 1, Rf_mkChar("POSIXt"));

        Rf_classgets(colVec, classes);

        UNPROTECT(2); // classes, colVec

        return;
      }

      UNPROTECT(1); // colVec
    }

    ~DoubleColumn()
    {
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
          SEXP timeZone = PROTECT(Rf_ScalarString(Rf_mkCharLen(annotation.c_str(), annotation.length())));
          Rf_setAttrib(colVec, Rf_install("tzone"), timeZone);
          UNPROTECT(1);
          return;
        }

        SEXP empty_str = PROTECT(Rf_mkString(""));
        Rf_setAttrib(colVec, Rf_install("tzone"), empty_str);
        UNPROTECT(1);
        return;
      }
    }
};


class IntegerColumn : public IIntegerColumn
{
  FstColumnAttribute columnAttribute;

public:
  SEXP colVec;

  IntegerColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    colVec = PROTECT(Rf_allocVector(INTSXP, (R_xlen_t) nrOfRows));

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

      UNPROTECT(1); // colVec
      return;
    }

    if (columnAttribute == FstColumnAttribute::INT_32_DATE_DAYS)
    {
      SEXP classAttr = PROTECT(Rf_allocVector(STRSXP, 2));

      SET_STRING_ELT(classAttr, 0, Rf_mkChar("IDate"));
      SET_STRING_ELT(classAttr, 1, Rf_mkChar("Date"));

      Rf_classgets(colVec, classAttr);

      UNPROTECT(2); // classAttr, colVec

      return;
    }

    if (columnAttribute == FstColumnAttribute::INT_32_TIMESTAMP_SECONDS)
    {
      SEXP classes = PROTECT(Rf_allocVector(STRSXP, 2));
      SET_STRING_ELT(classes, 0, Rf_mkChar("POSIXct"));
      SET_STRING_ELT(classes, 1, Rf_mkChar("POSIXt"));

      Rf_classgets(colVec, classes);

      UNPROTECT(2); // classAttr, colVec

      return;
    }

    if (columnAttribute == FstColumnAttribute::INT_32_TIME_OF_DAY_SECONDS)
    {
      Rf_classgets(colVec, Rf_mkString("ITime"));

      if (scale != FstTimeScale::SECONDS)
      {
        UNPROTECT(1); // colVec
        throw(std::runtime_error("ITime column with unknown scale detected"));
      }

      UNPROTECT(1); // colVec
      return;
    }

    UNPROTECT(1); // colVec
  }

  ~IntegerColumn()
  {
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
        SEXP annotation_str = PROTECT(Rf_ScalarString(Rf_mkCharLen(annotation.c_str(), annotation.length())));

        Rf_setAttrib(colVec, Rf_install("tzone"), annotation_str);

        UNPROTECT(1);  // annotation_str
        return;
      }

      SEXP empty_str = PROTECT(Rf_mkString(""));
      Rf_setAttrib(colVec, Rf_install("tzone"), empty_str);

      UNPROTECT(1);  // empty_str
      return;
    }
  }
};


class ByteColumn : public IByteColumn
{
public:
  SEXP colVec;

  ByteColumn(uint64_t nrOfRows)
  {
    // Note that this SEXP needs to be protected after creation
    colVec = Rf_allocVector(RAWSXP, (R_xlen_t) nrOfRows);
  }

  ~ByteColumn()
  {
  }

  char* Data()
  {
    return (char*) RAW(colVec);
  }
};


#endif  // FST_COLUMN_H
