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


#include <flex_store_v1.h>

#include <iostream>
#include <fstream>

#include <Rcpp.h>

#include <compression/compressor.h>

#include <character/character_v1.h>
#include <integer/integer_v2.h>
#include <double/double_v3.h>
#include <logical/logical_v4.h>
#include <factor/factor_v5.h>


using namespace std;
using namespace Rcpp;


// Scans further than necessary for safety!!!
List fstMeta_v1(String fileName)
{
  // Open file
  ifstream myfile;
  myfile.open(fileName.get_cstring(), ios::binary);

  // Additional check compared to CRAN v0.7.2
  if (myfile.fail())
  {
    myfile.close();
    ::Rf_error("There was an error opening the fst file. Please check for a correct filename.");
  }


  // Read column size
  short int colSizes[2];
  myfile.read((char*) &colSizes, 2 * sizeof(short int));


  // Additional checks compared to CRAN v0.7.2
  if ((colSizes[0] < 0) | (colSizes[1] < 0))
  {
    myfile.close();
    ::Rf_error("Unrecognised file type, are you sure this is a fst file?");
  }

  short int nrOfCols = colSizes[0];
  short int keyLength = colSizes[1] & 32767;


  // Read key column index
  short int *keyColumns = new short int[keyLength + 1];
  myfile.read((char*) keyColumns, keyLength * sizeof(short int));  // may be of length zero

  // Additional checks compared to CRAN v0.7.2
  if (keyLength > 0)
  {
    for (int keyColCount = 0; keyColCount < keyLength; ++keyColCount)
    {
      if ((keyColumns[keyColCount] < 0) | (keyColumns[keyColCount] >= nrOfCols))
      {
        myfile.close();
        delete[] keyColumns;
        ::Rf_error("Error reading file header, are you sure this is a fst file?");
      }
    }
  }


  // Read column types
  short int *colTypes = new short int[nrOfCols];
  myfile.read((char*) colTypes, nrOfCols * sizeof(short int));

  // Additional checks compared to CRAN v0.7.2
  for (int colCount = 0; colCount < nrOfCols; ++colCount)
  {
    if ((colTypes[colCount] < 0) | (colTypes[colCount] > 5))
    {
      myfile.close();
      delete[] colTypes;
      delete[] keyColumns;
      ::Rf_error("Error reading file header, are you sure this is a fst file?");
    }
  }


  // Convert to integer vector
  IntegerVector colTypeVec(nrOfCols);
  for (int col = 0; col != nrOfCols; ++col)
  {
    colTypeVec[col] = colTypes[col];
  }


  // Read block positions
  unsigned long long* allBlockPos = new unsigned long long[nrOfCols + 1];
  myfile.read((char*) allBlockPos, (nrOfCols + 1) * sizeof(unsigned long long));

  // Additional checks compared to CRAN v0.7.2
  for (int colCount = 2; colCount <= nrOfCols; ++colCount)
  {
    // block positions should be monotonically increasing
    if (allBlockPos[colCount] < allBlockPos[colCount - 1])
    {
      myfile.close();
      delete[] colTypes;
      delete[] keyColumns;
      delete[] allBlockPos;
      ::Rf_error("Error reading file header (blockPos), are you sure this is a fst file?");
    }
  }

  int nrOfRows = (int) allBlockPos[0];

  // Additional checks compared to CRAN v0.7.2
  if (nrOfRows <= 0)
  {
    myfile.close();
    delete[] colTypes;
    delete[] keyColumns;
    delete[] allBlockPos;
    ::Rf_error("Error reading file header (blockPos), are you sure this is a fst file?");
  }


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
  fdsReadCharVec_v1(myfile, colNames, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);

  // cleanup
  delete[] colTypes;
  delete[] allBlockPos;
  myfile.close();

  // add deprecated warning
  Rf_warning("This fst file was created with a beta version of the fst package. Please re-write the data as this format will not be supported in future releases.");

  if (keyLength > 0)
  {
    SEXP keyNames;
    PROTECT(keyNames = Rf_allocVector(STRSXP, keyLength));
    for (int i = 0; i < keyLength; ++i)
    {
      SET_STRING_ELT(keyNames, i, STRING_ELT(colNames, keyColumns[i]));
    }

    IntegerVector keyColIndex(keyLength);
    for (int col = 0; col != keyLength; ++col)
    {
      keyColIndex[col] = keyColumns[col];
    }

    UNPROTECT(2);
    delete[] keyColumns;

    return List::create(
      _["nrOfCols"]        = nrOfCols,
      _["nrOfRows"]        = nrOfRows,
      _["fstVersion"]      = 0,
      _["keyLength"]       = keyLength,
      _["colBaseType"]     = colTypeVec,
      _["colNames"]        = colNames,
      _["keyColIndex"]     = keyColIndex,
      _["keyNames"]        = keyNames);
  }

  UNPROTECT(1);
  delete[] keyColumns;

  return List::create(
    _["nrOfCols"]        = nrOfCols,
    _["nrOfRows"]        = nrOfRows,
    _["fstVersion"]      = 0,
    _["keyLength"]       = keyLength,
    _["colBaseType"]     = colTypeVec,
    _["colNames"]        = colNames);
}


SEXP fstRead_v1(String fileName, SEXP columnSelection, SEXP startRow, SEXP endRow)
{
  // Open file
  ifstream myfile;
  myfile.open(fileName.get_cstring(), ios::binary);

  // Additional check compared to CRAN v0.7.2
  if (myfile.fail())
  {
    myfile.close();
    ::Rf_error("There was an error opening the fst file. Please check for a correct filename.");
  }

  // Read column size
  short int colSizes[2];
  myfile.read((char*) &colSizes, 2 * sizeof(short int));


  // Additional checks compared to CRAN v0.7.2
  if ((colSizes[0] < 0) | (colSizes[1] < 0))
  {
    myfile.close();
    ::Rf_error("Unrecognised file type, are you sure this is a fst file?");
  }

  short int nrOfCols = colSizes[0];
  short int keyLength = colSizes[1] & 32767;


  // Read key column index
  short int *keyColumns = new short int[keyLength + 1];
  myfile.read((char*) keyColumns, keyLength * sizeof(short int));  // may be of length zero

  // Additional checks compared to CRAN v0.7.2
  if (keyLength > 0)
  {
    for (int keyColCount = 0; keyColCount < keyLength; ++keyColCount)
    {
      if ((keyColumns[keyColCount] < 0) | (keyColumns[keyColCount] >= nrOfCols))
      {
        myfile.close();
        delete[] keyColumns;
        ::Rf_error("Error reading file header, are you sure this is a fst file?");
      }
    }
  }

  // Read column types
  short int *colTypes = new short int[nrOfCols];
  myfile.read((char*) colTypes, nrOfCols * sizeof(short int));

  // Additional checks compared to CRAN v0.7.2
  for (int colCount = 0; colCount < nrOfCols; ++colCount)
  {
    if ((colTypes[colCount] < 0) | (colTypes[colCount] > 5))
    {
      myfile.close();
      delete[] colTypes;
      delete[] keyColumns;
      ::Rf_error("Error reading file header, are you sure this is a fst file?");
    }
  }


  // Read block positions
  unsigned long long* allBlockPos = new unsigned long long[nrOfCols + 1];
  myfile.read((char*) allBlockPos, (nrOfCols + 1) * sizeof(unsigned long long));

  // Additional checks compared to CRAN v0.7.2
  for (int colCount = 2; colCount <= nrOfCols; ++colCount)
  {
    // block positions should be monotonically increasing
    if (allBlockPos[colCount] < allBlockPos[colCount - 1])
    {
      myfile.close();
      delete[] colTypes;
      delete[] keyColumns;
      delete[] allBlockPos;
      ::Rf_error("Error reading file header (blockPos), are you sure this is a fst file?");
    }
  }

  // Read column names
  SEXP colNames;
  PROTECT(colNames = Rf_allocVector(STRSXP, nrOfCols));
  unsigned long long offset = (nrOfCols + 1) * sizeof(unsigned long long) + (nrOfCols + keyLength + 2) * sizeof(short int);
  fdsReadCharVec_v1(myfile, colNames, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);


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
        singleColInfo = fdsReadCharVec_v1(myfile, strVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, strVec);
        UNPROTECT(1);
        break;

      // Integer vector
      case 2:
        SEXP intVec;
        PROTECT(intVec = Rf_allocVector(INTSXP, length));
        singleColInfo = fdsReadIntVec_v2(myfile, intVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, intVec);
        UNPROTECT(1);
        break;

      // Real vector
      case 3:
        SEXP realVec;
        PROTECT(realVec = Rf_allocVector(REALSXP, length));
        singleColInfo = fdsReadRealVec_v3(myfile, realVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, realVec);
        UNPROTECT(1);
        break;

      // Logical vector
      case 4:
        SEXP boolVec;
        PROTECT(boolVec = Rf_allocVector(LGLSXP, length));
        singleColInfo = fdsReadLogicalVec_v4(myfile, boolVec, pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, boolVec);
        UNPROTECT(1);
        break;

      // Factor vector
      case 5:
        SEXP facVec;
        PROTECT(facVec = Rf_allocVector(INTSXP, length));
        singleColInfo = fdsReadFactorVec_v5(myfile, facVec, pos, firstRow, length, nrOfRows);
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

  // add deprecated warning
  Rf_warning("This fst file was created with a beta version of the fst package. Please re-write the data as this format will not be supported in future releases.");

  return List::create(
    _["keyNames"] = R_NilValue,
    _["selectedNames"] = selectedNames,
    _["resTable"] = resTable,
    _["colInfo"] = colInfo);
}
