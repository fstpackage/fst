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

#include <FastStore.h>

#include <Rcpp.h>
#include <iostream>
#include <fstream>
#include <R.h>
#include <Rinternals.h>

#include <lowerbound.h>
#include <charStore.h>
#include <factStore.h>
#include <intStore.h>
#include <doubleStore.h>
#include <boolStore.h>
#include <compression.h>
#include <compressor.h>

// External libraries
#include "lz4.h"
#include "zstd.h"

using namespace std;
using namespace Rcpp;

#define BLOCKSIZE 16384  // number of bytes in default compression block


SEXP fstStore(String fileName, SEXP table, SEXP compression)
{
  // Create file before using it (speed up)
  ofstream myfile;
  myfile.open(fileName.get_cstring(), ios::binary);

  if (myfile.fail())
  {
    myfile.close();
    ::Rf_error("There was an error creating the file. Please check for a correct filename.");
  }

  SEXP colNames = Rf_getAttrib(table, R_NamesSymbol);
  SEXP keyNames = Rf_getAttrib(table, Rf_mkString("sorted"));

  if (!Rf_isInteger(compression))
  {
    myfile.close();
    ::Rf_error("Parameter compression should be an integer value between 0 and 100");
  }

  int compress = *INTEGER(compression);
  if ((compress < 0) | (compress > 100))
  {
    myfile.close();
    ::Rf_error("Parameter compression should be an integer value between 0 and 100");
  }

  // Meta on dataset
  int keyLength = 0;
  short int nrOfCols = LENGTH(colNames);

  if (nrOfCols == 0)
  {
    myfile.close();
    ::Rf_error("Your dataset needs at least one column.");
  }

  short int *metaData = new short int[nrOfCols + keyLength + 2];

  metaData[0] = nrOfCols;

  if (!Rf_isNull(keyNames))
  {
    keyLength = LENGTH(keyNames);

    // Determine key column indexes
    int equal;
    for (int colSel = 0; colSel < keyLength; ++colSel)
    {
      equal = -1;
      const char* str1 = CHAR(STRING_ELT(keyNames, colSel));

      for (int colNr = 0; colNr < nrOfCols; ++colNr)
      {
        const char* str2 = CHAR(STRING_ELT(colNames, colNr));
        if (strcmp(str1, str2) == 0)
        {
          equal = colNr;
          break;
        }
      }
      metaData[2 + colSel] = equal;
    }
  }

  metaData[1] = keyLength;

  // Pointers to column file positions
  unsigned long long *positionData = new unsigned long long[nrOfCols + 1];

  SEXP firstCol = VECTOR_ELT(table, 0);
  int nrOfRows = LENGTH(firstCol);
  positionData[0] = nrOfRows;

  myfile.write((char*)(metaData), sizeof(short int) * (nrOfCols + keyLength + 2));
  myfile.write((char*)(positionData), sizeof(unsigned long long) * (nrOfCols + 1));  // chunk column positions
  fdsWriteCharVec(myfile, colNames, nrOfCols, 0);  // column names

  if (nrOfRows == 0)
  {
    delete[] metaData;
    delete[] positionData;
    myfile.close();
    ::Rf_error("The dataset contains no data.");
  }

  SEXP colResult = NULL;

  // column data
  for (int colNr = 0; colNr < nrOfCols; ++colNr)
  {
    positionData[colNr + 1] = myfile.tellp();  // current location
    SEXP colVec = VECTOR_ELT(table, colNr);

    switch (TYPEOF(colVec))
    {
      case STRSXP:
        metaData[2 + keyLength + colNr] = 1;
        colResult = fdsWriteCharVec(myfile, colVec, nrOfRows, compress);
        break;

      case INTSXP:
        if(Rf_isFactor(colVec))
        {
          metaData[2 + keyLength + colNr] = 5;
          colResult = fdsWriteFactorVec(myfile, colVec, nrOfRows, compress);
          break;
        }

        metaData[2 + keyLength + colNr] = 2;
        colResult = fdsWriteIntVec(myfile, colVec, nrOfRows, compress);
        break;

      case REALSXP:
        metaData[2 + keyLength + colNr] = 3;
        colResult = fdsWriteRealVec(myfile, colVec, nrOfRows, compress);
        break;

      case LGLSXP:
        metaData[2 + keyLength + colNr] = 4;
        colResult = fdsWriteLogicalVec(myfile, colVec, nrOfRows, compress);
        break;

      default:
        delete[] metaData;
        delete[] positionData;
        myfile.close();
        ::Rf_error("Unknown type found in column.");
    }
  }

  myfile.seekp(0);
  myfile.write((char*)(metaData), sizeof(short int) * (nrOfCols + keyLength + 2));
  myfile.write((char*)(positionData), sizeof(unsigned long long) * (nrOfCols + 1));

  myfile.close();

  // cleanup
  delete[] metaData;
  delete[] positionData;

  return List::create(
    _["keyNames"] = keyNames,
    _["keyLength"] = keyLength,
    _["colResult"] = colResult);
}


List fstMeta(String fileName)
{
  // Open file
  ifstream myfile;
  myfile.open(fileName.get_cstring(), ios::binary);


  // Read column size
  short int colSizes[2];
  myfile.read((char*) &colSizes, 2 * sizeof(short int));
  short int nrOfCols = colSizes[0];
  short int keyLength = colSizes[1] & 32767;


  // Read key column index
  short int *keyColumns = new short int[keyLength + 1];
  myfile.read((char*) keyColumns, keyLength * sizeof(short int));  // may be of length zero

  // Convert to integer vector
  IntegerVector keyColVec(keyLength);
  for (int col = 0; col != keyLength; ++col)
  {
    keyColVec[col] = keyColumns[col];
  }


  // Read column types
  short int *colTypes = new short int[nrOfCols];
  myfile.read((char*) colTypes, nrOfCols * sizeof(short int));

  // Convert to integer vector
  IntegerVector colTypeVec(nrOfCols);
  for (int col = 0; col != nrOfCols; ++col)
  {
    colTypeVec[col] = colTypes[col];
  }


  // Read block positions
  unsigned long long* allBlockPos = new unsigned long long[nrOfCols + 1];
  myfile.read((char*) allBlockPos, (nrOfCols + 1) * sizeof(unsigned long long));

  // Convert to numeric vector
  NumericVector blockPosVec(nrOfCols + 1);
  for (int col = 0; col != nrOfCols + 1; ++col)
  {
    blockPosVec[col] = (double) allBlockPos[col];
  }


  // Read column names
  SEXP colNames;
  PROTECT(colNames = Rf_allocVector(STRSXP, nrOfCols));
  unsigned long long offset = (nrOfCols + 1) * sizeof(unsigned long long) + (nrOfCols + keyLength + 2) * sizeof(short int);
  fdsReadCharVec(myfile, colNames, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);

  // cleanup
  delete[] keyColumns;
  delete[] colTypes;
  delete[] allBlockPos;
  myfile.close();
  UNPROTECT(1);

  return List::create(
    _["nrOfCols"]    = nrOfCols,
    _["keyLength"]   = keyLength,
    _["keyColVec"]   = keyColVec,
    _["colTypeVec"]  = colTypeVec,
    _["blockPosVec"] = blockPosVec,
    _["colNames"] = colNames);
}


SEXP fstRead(SEXP fileName, SEXP columnSelection, SEXP startRow, SEXP endRow)
{
  // Open file
  ifstream myfile;
  myfile.open(CHAR(STRING_ELT(fileName, 0)), ios::binary);


  // Read column size
  short int colSizes[2];
  myfile.read((char*) &colSizes, 2 * sizeof(short int));
  short int nrOfCols = colSizes[0];
  short int keyLength = colSizes[1] & 32767;


  // Read key column index
  short int *keyColumns = new short int[keyLength + 1];
  myfile.read((char*) keyColumns, keyLength * sizeof(short int));  // may be of length zero


  // Read column types
  short int *colTypes = new short int[nrOfCols];
  myfile.read((char*) colTypes, nrOfCols * sizeof(short int));


  // Read block positions
  unsigned long long* allBlockPos = new unsigned long long[nrOfCols + 1];
  myfile.read((char*) allBlockPos, (nrOfCols + 1) * sizeof(unsigned long long));


  // Read column names
  SEXP colNames;
  PROTECT(colNames = Rf_allocVector(STRSXP, nrOfCols));
  unsigned long long offset = (nrOfCols + 1) * sizeof(unsigned long long) + (nrOfCols + keyLength + 2) * sizeof(short int);
  fdsReadCharVec(myfile, colNames, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);


  // Determine column selection
  int *colIndex = new int[nrOfCols];
  int nrOfSelect = LENGTH(columnSelection);

  if (Rf_isNull(columnSelection))
  {
    for (int colNr = 0; colNr < nrOfCols; ++colNr)
    {
      colIndex[colNr] = colNr;
    }
    nrOfSelect = nrOfCols;
  }
  else  // determine column numbers of column names
  {
    int equal;
    for (int colSel = 0; colSel < nrOfSelect; ++colSel)
    {
      equal = -1;
      const char* str1 = CHAR(STRING_ELT(columnSelection, colSel));

      for (int colNr = 0; colNr < nrOfCols; ++colNr)
      {
        const char* str2 = CHAR(STRING_ELT(colNames, colNr));
        if (strcmp(str1, str2) == 0)
        {
          equal = colNr;
          break;
        }
      }

      if (equal == -1)
      {
        delete[] colIndex;
        delete[] colTypes;
        delete[] keyColumns;
        delete[] allBlockPos;
        myfile.close();
        UNPROTECT(1);
        ::Rf_error("Selected column not found.");
      }

      colIndex[colSel] = equal;
    }
  }


  // Check range of selected rows
  int firstRow = INTEGER(startRow)[0] - 1;
  int nrOfRows = (int) allBlockPos[0];

  if (firstRow >= nrOfRows || firstRow < 0)
  {
    delete[] colIndex;
    delete[] colTypes;
    delete[] keyColumns;
    delete[] allBlockPos;
    myfile.close();
    UNPROTECT(1);

    if (firstRow < 0)
    {
      ::Rf_error("Parameter fromRow should have a positive value.");
    }

    ::Rf_error("Row selection is out of range.");
  }

  int length = nrOfRows - firstRow;

  // Determine vector length
  if (!Rf_isNull(endRow))
  {
    int lastRow = *INTEGER(endRow);

    if (lastRow <= firstRow)
    {
      delete[] colIndex;
      delete[] colTypes;
      delete[] keyColumns;
      delete[] allBlockPos;
      myfile.close();
      UNPROTECT(1);
      ::Rf_error("Parameter 'lastRow' should be equal to or larger than parameter 'fromRow'.");
    }

    length = min(lastRow - firstRow, nrOfRows - firstRow);
  }

  unsigned long long* blockPos = allBlockPos + 1;

  // Vector of selected column names
  SEXP selectedNames;
  PROTECT(selectedNames = Rf_allocVector(STRSXP, nrOfSelect));

  SEXP resTable;
  PROTECT(resTable = Rf_allocVector(VECSXP, nrOfSelect));

  SEXP colInfo;
  PROTECT(colInfo = Rf_allocVector(VECSXP, nrOfSelect));

  for (int colSel = 0; colSel < nrOfSelect; ++colSel)
  {
    int colNr = colIndex[colSel];

    if (colNr < 0 || colNr >= nrOfCols)
    {
      delete[] colIndex;
      delete[] colTypes;
      delete[] keyColumns;
      delete[] allBlockPos;
      myfile.close();
      UNPROTECT(4);
      ::Rf_error("Column selection is out of range.");
    }

    // Column name
    SEXP selName = STRING_ELT(colNames, colNr);
    SET_STRING_ELT(selectedNames, colSel, selName);

    unsigned long long pos = blockPos[colNr];

    SEXP singleColInfo;

    switch (colTypes[colNr])
    {
    // Character vector
      case 1:
        SEXP strVec;
        PROTECT(strVec = Rf_allocVector(STRSXP, length));
        singleColInfo = fdsReadCharVec(myfile, strVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, strVec);
        UNPROTECT(1);
        break;

      // Integer vector
      case 2:
        SEXP intVec;
        PROTECT(intVec = Rf_allocVector(INTSXP, length));
        singleColInfo = fdsReadIntVec(myfile, intVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, intVec);
        UNPROTECT(1);
        break;

      // Real vector
      case 3:
        SEXP realVec;
        PROTECT(realVec = Rf_allocVector(REALSXP, length));
        singleColInfo = fdsReadRealVec(myfile, realVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, realVec);
        UNPROTECT(1);
        break;

      // Logical vector
      case 4:
        SEXP boolVec;
        PROTECT(boolVec = Rf_allocVector(LGLSXP, length));
        singleColInfo = fdsReadLogicalVec(myfile, boolVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, boolVec);
        UNPROTECT(1);
        break;

      // Factor vector
      case 5:
        SEXP facVec;
        PROTECT(facVec = Rf_allocVector(INTSXP, length));
        singleColInfo = fdsReadFactorVec(myfile, facVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, facVec);
        UNPROTECT(2);  // level string was also generated
        break;


      default:
        delete[] colIndex;
        delete[] colTypes;
        delete[] keyColumns;
        delete[] allBlockPos;
        myfile.close();
        ::Rf_error("Unknown type found in column.");
    }

    SET_VECTOR_ELT(colInfo, colSel, singleColInfo);
  }

  myfile.close();

  Rf_setAttrib(resTable, R_NamesSymbol, selectedNames);

  int found = 0;
  for (int i = 0; i < keyLength; ++i)
  {
    for (int colSel = 0; colSel < nrOfSelect; ++colSel)
    {
      if (keyColumns[i] == colIndex[colSel])  // key present in result
      {
        ++found;
        break;
      }
    }
  }

  // Only keys present in result set
  if (found > 0)
  {
    SEXP keyNames;
    PROTECT(keyNames = Rf_allocVector(STRSXP, found));
    for (int i = 0; i < found; ++i)
    {
      SET_STRING_ELT(keyNames, i, STRING_ELT(colNames, keyColumns[i]));
    }

    // cleanup
    UNPROTECT(5);
    delete[] colIndex;
    delete[] colTypes;
    delete[] keyColumns;
    delete[] allBlockPos;

    return List::create(
      _["keyNames"] = keyNames,
      _["resTable"] = resTable,
      _["selectedNames"] = selectedNames,
      _["colInfo"] = colInfo);
  }

  UNPROTECT(4);
  delete[] colIndex;
  delete[] colTypes;
  delete[] keyColumns;
  delete[] allBlockPos;

  return List::create(
    _["keyNames"] = R_NilValue,
    _["selectedNames"] = selectedNames,
    _["resTable"] = resTable,
    _["colInfo"] = colInfo);
}
