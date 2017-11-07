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

#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <memory>

#include <Rcpp.h>

#include <interface/fstdefines.h>
#include <interface/istringwriter.h>
#include <interface/ifsttable.h>
#include <interface/icolumnfactory.h>
#include <interface/fststore.h>

#include <fst_error.h>
#include <fst_blockrunner_char.h>
#include <fst_table.h>
#include <fst_column.h>
#include <fst_column_factory.h>

#include <flex_store.h>
#include <flex_store_v1.h>


using namespace std;
using namespace Rcpp;


inline int FindKey(StringVector colNameList, String item)
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


SEXP fststore(String fileName, SEXP table, SEXP compression, SEXP uniformEncoding)
{
  if (!Rf_isLogical(uniformEncoding))
  {
    return fst_error("Parameter uniform.encoding should be a logical value");
  }

  if (!Rf_isInteger(compression))
  {
    return fst_error("Parameter compression should be an integer value between 0 and 100");
  }

  int compress = *INTEGER(compression);
  if ((compress < 0) | (compress > 100))
  {
    return fst_error("Parameter compression should be an integer value between 0 and 100");
  }

  FstTable fstTable(table, *LOGICAL(uniformEncoding));
  FstStore fstStore(fileName.get_cstring());

  try
  {
    fstStore.fstWrite(fstTable, compress);
  }
  catch (const std::runtime_error& e)
  {
    return fst_error(e.what());
  }

  return R_NilValue;
}


SEXP fstmetadata(String fileName)
{
  FstStore fstStore(fileName.get_cstring());
  std::unique_ptr<IColumnFactory> columnFactory(new ColumnFactory());

  try
  {
    fstStore.fstMeta(columnFactory.get());

  }
  catch (const std::runtime_error& e)
  {
    // We may be looking at a fst v0.7.2 file format, this unsafe code will be removed later
    if (std::strcmp(e.what(), FSTERROR_NON_FST_FILE) == 0)
    {
      List resOld = fstMeta_v1(fileName);  // scans further for safety

      IntegerVector typeVec = resOld[4];

      // change type ordering
      int types[6] { 0, 2, 4, 5, 6, 3};
      for (int pos = 0; pos < typeVec.size(); pos++)
      {
        int oldType = typeVec[pos];
        typeVec[pos] = types[oldType];
      }

      resOld[4] = typeVec;
      return resOld;  // scans further for safety
    }

    return fst_error(e.what());
  }

  // R internals part TODO: speed up this code
  SEXP colNames = ((BlockReaderChar*) fstStore.blockReader)->StrVector();

  // Convert column info to integer vector
  // IntegerVector colTypeVec(fstStore.nrOfCols);
  IntegerVector colBaseType(fstStore.nrOfCols);
  IntegerVector colAttributeTypes(fstStore.nrOfCols);

  for (int col = 0; col != fstStore.nrOfCols; ++col)
  {
    // colTypeVec[col] = fstStore.colTypes[col];
    colBaseType[col] = fstStore.colBaseTypes[col];
    colAttributeTypes[col] = fstStore.colAttributeTypes[col];
  }

  List retList;

  if (fstStore.keyLength > 0)
  {
    SEXP keyNames;
    PROTECT(keyNames = Rf_allocVector(STRSXP, fstStore.keyLength));
    for (int i = 0; i < fstStore.keyLength; ++i)
    {
      SET_STRING_ELT(keyNames, i, STRING_ELT(colNames, fstStore.keyColPos[i]));
    }

    IntegerVector keyColIndex(fstStore.keyLength);
    for (int col = 0; col != fstStore.keyLength; ++col)
    {
      keyColIndex[col] = fstStore.keyColPos[col];
    }

    UNPROTECT(1);  // keyNames

    retList = List::create(
      _["nNofCols"]         = fstStore.nrOfCols,
      _["nrOfRows"]         = *(fstStore.p_nrOfRows),
      // _["fstVersion"]       = fstStore.version,
      _["keyLength"]        = fstStore.keyLength,
      _["colBaseType"]      = colBaseType,
      _["colType"]          = colAttributeTypes,
      _["colNames"]         = colNames,
      _["keyColIndex"]      = keyColIndex,
      _["keyNames"]         = keyNames);
  }
  else
  {
    retList = List::create(
      _["nrOfCols"]        = fstStore.nrOfCols,
      _["nrOfRows"]        = *fstStore.p_nrOfRows,
      // _["fstVersion"]      = fstStore.version,
      _["keyLength"]       = fstStore.keyLength,
      _["colBaseType"]     = colBaseType,
      _["colType"]         = colAttributeTypes,
      _["colNames"]        = colNames);
  }

  return retList;
}


SEXP fstretrieve(String fileName, SEXP columnSelection, SEXP startRow, SEXP endRow)
{
  FstTable tableReader;
  std::unique_ptr<IColumnFactory> columnFactory(new ColumnFactory());
  FstStore fstStore(fileName.get_cstring());

  int sRow = *INTEGER(startRow);

  // Set to last row
  int eRow = -1;

  if (!Rf_isNull(endRow))
  {
    eRow = *INTEGER(endRow);
  }

  vector<int> keyIndex;

  std::unique_ptr<StringArray> colNames(new StringArray());
  std::unique_ptr<StringArray> colSelection;

  if (!Rf_isNull(columnSelection))
  {
    colSelection = std::unique_ptr<StringArray>(new StringArray());
    colSelection->SetArray(columnSelection);
  }

  int result = 0;

  try
  {
    fstStore.fstRead(tableReader, colSelection.get(), sRow, eRow, columnFactory.get(), keyIndex, &*colNames);
  }
  catch (const std::runtime_error& e)
  {
    // We may be looking at a fst v0.7.2 file format, this unsafe code will be removed later
    if (std::strcmp(e.what(), FSTERROR_NON_FST_FILE) == 0)
    {
      result = -1;
    }
    else
    {
      // Re-throw uncatched errors
      return fst_error(e.what());
    }
  }


  // Test deprecated version format !!!
  if (result == -1)
  {
    try
    {
      SEXP res = fstRead_v1(fileName, columnSelection, startRow, endRow);

      Rf_warning("This fst file was created with a beta version of the fst package. Please re-write the data as this format will not be supported in future releases.");

      return res;
    }
    catch (const std::exception& ex)
    {
      // Re-throw uncatched errors
      return fst_error(ex.what());
    }
  }

  SEXP colNameVec = colNames->StrVector();

  // Generalize to full atributes
  Rf_setAttrib(tableReader.resTable, R_NamesSymbol, colNameVec);


  // Convert keyIndex to keyNames
  SEXP keyNames;
  PROTECT(keyNames = Rf_allocVector(STRSXP, keyIndex.size()));

  int count = 0;

  for (vector<int>::iterator keyIt = keyIndex.begin(); keyIt != keyIndex.end(); ++keyIt)
  {
    SET_STRING_ELT(keyNames, count++, STRING_ELT(colNameVec, *keyIt));
  }

  UNPROTECT(1);

  return List::create(
    _["keyNames"]   = keyNames,
    _["keyIndex"]   = keyIndex,
    _["colNameVec"] = colNameVec,
    _["resTable"]   = tableReader.resTable);
}
