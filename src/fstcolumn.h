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

#include "ifstcolumn.h"
#include "blockrunner_char.h"
//
// class FactorColumn
// {
// public:
//   SEXP intVec;
//   BlockReaderChar* blockReaderStrVec;
//
//   FactorColumn(int nrOfRows)
//   {
//     intVec = Rf_allocVector(INTSXP, nrOfRows);
//     PROTECT(intVec);
//     blockReaderStrVec = new BlockReaderChar();
//   }
//
//   ~IFactorColumn()
//   {
//     UNPROTECT(1);
//     delete blockReaderStrVec;
//   };
//
//   int* LevelData()
//   {
//     return INTEGER(intVec);
//   }
//
//   IBlockReader* Levels()
//   {
//     return blockReaderStrVec;
//   }
// };



class LogicalColumn : public ILogicalColumn
{
public:
  SEXP colVec;

  LogicalColumn(int nrOfRows)
  {
    colVec = Rf_allocVector(LGLSXP, nrOfRows);
    PROTECT(colVec);
  }

  ~LogicalColumn()
  {
    UNPROTECT(1);
  }

  int* Data()
  {
    return LOGICAL(colVec);
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


#endif  // FST_COLUMN_H
